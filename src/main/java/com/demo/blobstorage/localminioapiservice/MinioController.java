package com.demo.blobstorage.localminioapiservice;

import io.minio.BucketExistsArgs;
import io.minio.MinioClient;
import io.minio.MakeBucketArgs;
import io.minio.UploadObjectArgs;
import io.minio.errors.MinioException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

@Slf4j
@RestController
@RequestMapping("minio")
public class MinioController {

    @Value("${app.blob-storage.local-directory}")
    private String localDirectory;

    @Value("${app.blob-storage.local-filename}")
    private String localFilename;

    @Value("${app.blob-storage.upload-filename}")
    private String uploadFilename;

    @Value("${app.blob-storage.upload-bucketname}")
    private String bucketName;

    @Value("${app.blob-storage.access.accesskey}")
    private String accessKey;

    @Value("${app.blob-storage.access.secretkey}")
    private String secretKey;

    @Value("${app.blob-storage.url}")
    private String storageUrl;

    @GetMapping("/listbuckets")
    public ResponseEntity<String> listBuckets() throws IOException, NoSuchAlgorithmException, InvalidKeyException {
        create();
        return ResponseEntity.ok("Upload Success");
    }

    public void create()
            throws IOException, NoSuchAlgorithmException, InvalidKeyException {
        try {
            MinioClient minioClient =
                    MinioClient.builder()
                            .endpoint(storageUrl)
                            .credentials(accessKey, secretKey)
                            .build();

            boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
            if (!found) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            } else {
                log.info("Bucket {} already exists.", bucketName);
            }

            String localFileName = localDirectory + localFilename;
            minioClient.uploadObject(
                    UploadObjectArgs.builder()
                            .bucket(bucketName)
                            .object(uploadFilename)
                            .filename(localFileName)
                            .build());
            log.info("'{}' is successfully uploaded as object '{}' to bucket '{}'.", localFileName, bucketName, uploadFilename);
        } catch (MinioException e) {
            log.error("Error occurred: ", e);
            log.error("HTTP trace: {}", e.httpTrace());
        }
    }
}
