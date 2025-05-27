const fs = require('fs');
require('dotenv').config();
const path = require('path');
const { exec } = require('child_process');
const axios = require('axios');
const { MongoClient, ObjectId } = require('mongodb');
const { uploadFolderToS3 } = require('./upload_module');

const mongoUri = process.env.MONGO_URI;
const dbName = 'test';
const collectionName = 'processedredgifs';
const outputRoot = path.join(__dirname, 'videos');
const logDir = path.join(__dirname, 'logs');
const bucketName = 'zidit';

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

async function processEntry(entry, db) {
    const keyName = sanitizeKey(entry.videoUrl);
    const baseDir = path.join(outputRoot, keyName);
    const videosDir = path.join(baseDir, 'videos');
    const inputPath = path.join(baseDir, 'input.mp4');
    const outputPath = path.join(videosDir, 'output.m3u8');
    const metaPath = path.join(videosDir, 'meta.json');

    log(`🚀 Starting: ${keyName}`);

    if (entry.processed) {
        log(`⏭️ Already processed: ${keyName}`);
        return;
    }

    try {
        fs.mkdirSync(videosDir, { recursive: true });

        // Step 1: Download video
        log(`⬇️ Downloading video: ${entry.videoUrl}`);
        const writer = fs.createWriteStream(inputPath);
        const response = await axios.get(entry.videoUrl, { responseType: 'stream' });
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
        const s3Prefix = `hls/${keyName}/videos`;
        log(`☁️ Uploading to S3 at ${s3Prefix}/`);
        await uploadFolderToS3(videosDir, bucketName, s3Prefix);
        const hlsUrl = `https://${bucketName}.s3.${process.env.AWS_REGION}.amazonaws.com/${s3Prefix}/output.m3u8`;

        // Step 4: Save metadata
        const metadata = {
            id: entry._id,
            rawUrl: entry.rawUrl,
            videoUrl: entry.videoUrl,
            imageUrl: entry.imageUrl,
            username: entry.username,
            tags: entry.tags,
            description: entry.description,
            views: entry.views,
            hlsUrl: hlsUrl,
            processedAt: new Date().toISOString()
        };

        fs.writeFileSync(metaPath, JSON.stringify(metadata, null, 2));
        log(`📁 Saved metadata to ${metaPath}`);

        // Step 5: Move document to 'videos' collection and delete from 'processedredgifs'
        const newCollection = db.collection('videos');
        const oldCollection = db.collection(collectionName);

        await newCollection.insertOne(metadata);

        // Step 6: Delete the original video
        if (fs.existsSync(inputPath)) {
            fs.unlinkSync(inputPath);
            log(`🗑️ Deleted original video: ${inputPath}`);
        }

        log(`✅ Moved entry to 'videos' collection and cleaned up.`);

    } catch (err) {
        log(`❌ Failed to process ${keyName}: ${err.message}`);
    }
}

async function main() {
    const client = new MongoClient(mongoUri);

    try {
        await client.connect();
        const db = client.db(dbName);
        const collection = db.collection(collectionName);

        log(`🔍 Fetching unprocessed videos...`);
        const cursor = collection.find({ processed: { $ne: true } });

        while (await cursor.hasNext()) {
            const entry = await cursor.next();
            await processEntry(entry, db); // <-- pass db, not collection
        }

        log(`🏁 All entries processed.`);

    } catch (err) {
        log(`💥 MongoDB error: ${err.message}`);
    } finally {
        await client.close();
    }
}


main();
