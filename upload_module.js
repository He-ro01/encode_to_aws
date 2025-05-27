require('dotenv').config(); // Load env vars early

const fs = require('fs');
const path = require('path');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');

// Create an S3 client using environment variables
const s3 = new S3Client({
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
    }
});

// Your S3 usage continues here...


async function uploadFolderToS3(localFolder, bucket, prefix) {
    const files = fs.readdirSync(localFolder);

    for (const file of files) {
        const filePath = path.join(localFolder, file);
        if (fs.statSync(filePath).isFile()) {
            const content = fs.readFileSync(filePath);
            const key = path.join(prefix, file).replace(/\\/g, '/');

            const command = new PutObjectCommand({
                Bucket: bucket,
                Key: key,
                Body: content,
                ContentType: getMimeType(file)
            });

            await s3.send(command);
            console.log(`✅ Uploaded: ${key}`);
        }
    }
}

function getMimeType(filename) {
    if (filename.endsWith('.m3u8')) return 'application/vnd.apple.mpegurl';
    if (filename.endsWith('.ts')) return 'video/MP2T';
    return 'application/octet-stream';
}

module.exports = { uploadFolderToS3 };
