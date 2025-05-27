const fs = require('fs');
const path = require('path');
const AWS = require('aws-sdk');
const mime = require('mime');

AWS.config.update({
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    region: process.env.AWS_REGION
});

const s3 = new AWS.S3();

async function uploadFolderToS3(folderPath, bucketName, s3Prefix = '') {
    const uploadedKeys = [];

    async function uploadFile(filePath) {
        const fileStream = fs.createReadStream(filePath);
        const key = path.join(s3Prefix, path.relative(folderPath, filePath)).replace(/\\/g, '/');
        const contentType = mime.getType(filePath) || 'application/octet-stream';

        const params = {
            Bucket: bucketName,
            Key: key,
            Body: fileStream,
            ContentType: contentType,
            ACL: 'public-read'
        };

        await s3.upload(params).promise();
        uploadedKeys.push(key);
    }

    async function walkAndUpload(dir) {
        const entries = fs.readdirSync(dir, { withFileTypes: true });
        for (const entry of entries) {
            const fullPath = path.join(dir, entry.name);
            if (entry.isDirectory()) {
                await walkAndUpload(fullPath);
            } else {
                await uploadFile(fullPath);
            }
        }
    }

    await walkAndUpload(folderPath);
    return uploadedKeys;
}

module.exports = { uploadFolderToS3 };
