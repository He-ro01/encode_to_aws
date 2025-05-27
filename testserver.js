//test server
const fs = require('fs');
require('dotenv').config();
const path = require('path');
const { exec } = require('child_process');
const axios = require('axios');
const { MongoClient } = require('mongodb');
const { uploadFolderToS3 } = require('./upload_module');

const mongoUri = process.env.MONGO_URI;
const dbName = 'test';
const collectionName = 'videos';
const outputRoot = path.join(__dirname, 'videos');
const logDir = path.join(__dirname, 'logs');
const bucketName = 'zidit';
const inputFile = path.join(__dirname, 'testurls.txt');

if (!fs.existsSync(logDir)) fs.mkdirSync(logDir);
const logFile = path.join(logDir, `${new Date().toISOString().slice(0, 10)}.log`);

function log(message) {
    const timestamp = new Date().toISOString();
    const line = `[${timestamp}] ${message}`;
    console.log(line);
    fs.appendFileSync(logFile, line + '\n');
}

function sanitizeKey(url) {
    return url.replace(/^https?:\/\//, '').replace(/\.[^/.]+$/, '').replace(/[^a-zA-Z0-9]/g, '_');
}

async function processUrl(videoUrl, db) {
    const uniqueId = `${sanitizeKey(videoUrl)}_${Date.now()}`;
    const inputPath = path.join(outputRoot, `${uniqueId}_input.mp4`);
    const outputPath = path.join(outputRoot, `${uniqueId}.m3u8`);
    const metaPath = path.join(outputRoot, `${uniqueId}_meta.json`);

    log(`🚀 Starting: ${keyName}`);

    if (fs.existsSync(outputPath)) {
        log(`⏭️ Already processed: ${keyName}`);
        return;
    }

    try {
        if (!fs.existsSync(outputRoot)) fs.mkdirSync(outputRoot, { recursive: true });

        // Step 1: Download video
        log(`⬇️ Downloading video: ${videoUrl}`);
        const writer = fs.createWriteStream(inputPath);
        const response = await axios.get(videoUrl, { responseType: 'stream' });
        response.data.pipe(writer);
        await new Promise((res, rej) => {
            writer.on('finish', res);
            writer.on('error', rej);
        });

        // Step 2: FFmpeg HLS conversion
        log(`🎞️ Converting with FFmpeg`);
        const ffmpegCmd = `ffmpeg -i "${inputPath}" -codec copy -start_number 0 -hls_time 10 -hls_list_size 0 -f hls "${outputPath}"`;
        await new Promise((res, rej) => {
            exec(ffmpegCmd, (err, stdout, stderr) => {
                if (err) {
                    log(`❌ FFmpeg error: ${stderr}`);
                    return rej(err);
                }
                res();
            });
        });

        // Step 3: Upload to S3
        const s3Prefix = ''; // root upload
        log(`☁️ Uploading to S3 at ${s3Prefix}`);
        await uploadFolderToS3(outputRoot, bucketName, '');

        const hlsUrl = `${process.env.CLOUDFRONT_URL}/${uniqueId}.m3u8`;

        // Step 4: Save metadata
        const metadata = {
            rawUrl: videoUrl,
            hlsUrl: hlsUrl,
            processedAt: new Date().toISOString(),
        };

        fs.writeFileSync(metaPath, JSON.stringify(metadata, null, 2));
        log(`📁 Saved metadata to ${metaPath}`);

        // Step 5: Save to MongoDB
        const collection = db.collection(collectionName);
        await collection.insertOne(metadata);

        // Step 6: Delete original video
        if (fs.existsSync(inputPath)) {
            fs.unlinkSync(inputPath);
            log(`🗑️ Deleted original video: ${inputPath}`);
        }

        log(`✅ Successfully processed and saved to MongoDB: ${keyName}`);
    } catch (err) {
        log(`❌ Failed to process ${keyName}: ${err.message}`);
    }
}

async function main() {
    const client = new MongoClient(mongoUri);
    try {
        await client.connect();
        const db = client.db(dbName);

        log(`📄 Reading URLs from ${inputFile}`);
        const urls = fs.readFileSync(inputFile, 'utf-8')
            .split('\n')
            .map(line => line.trim())
            .filter(Boolean);

        for (const url of urls) {
            await processUrl(url, db);
        }

        log(`🏁 All URLs processed.`);
    } catch (err) {
        log(`💥 Error: ${err.message}`);
    } finally {
        await client.close();
    }
}

main();
