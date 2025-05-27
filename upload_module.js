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

// Upload all files in a folder to S3
async function uploadFolderToS3(localFolder, bucket, prefix) {
    const files = fs.readdirSync(localFolder);
    const uploadedKeys = [];

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

            // Only add to uploadedKeys if it’s an m3u8 file
            if (file.endsWith('.m3u8')) {
                uploadedKeys.push(key);
            }
        }
    }

    return uploadedKeys;
}

function getMimeType(filename) {
    if (filename.endsWith('.m3u8')) return 'application/vnd.apple.mpegurl';
    if (filename.endsWith('.ts')) return 'video/MP2T';
    return 'application/octet-stream';
}

module.exports = { uploadFolderToS3 };
