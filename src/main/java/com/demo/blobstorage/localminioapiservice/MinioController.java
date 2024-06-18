package com.demo.blobstorage.localminioapiservice;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.UploadObjectArgs;
import io.minio.errors.MinioException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("minio")
public class MinioController {

//    @Value("${app.blob-storage.local-directory}")
//    private String localDirectory;
//
//    @Value("${app.blob-storage.local-filename}")
//    private String localFilename;

//    @Value("${app.blob-storage.upload-filename}")
//    private String uploadFilename;

//    @Value("${app.blob-storage.upload-bucketname}")
//    private String bucketName;

    @Value("${app.blob-storage.access.accesskey}")
    private String accessKey;

    @Value("${app.blob-storage.access.secretkey}")
    private String secretKey;

    @Value("${app.blob-storage.url}")
    private String storageUrl;

    @Value("#{${app.blob-storage.upload-meta-data}}")
    private Map<String, String> metaDataProperty;

    @GetMapping("/upload-file")
    public ResponseEntity<String> listBuckets(
            @RequestParam("upload-file-name") String uploadFilename,
            @RequestParam("upload-bucket-name") String uploadBucketName,
            @RequestParam("local-file-name") String localFilename,
            @RequestParam("local-file-path") String localFilePath) throws Exception {
        boolean isSuccess = create(uploadFilename, uploadBucketName, localFilename, localFilePath);
        return isSuccess ? ResponseEntity.ok("Upload Success") : ResponseEntity.internalServerError().body("Upload Failed");
    }

    public boolean create(String uploadFilename, String uploadBucketName, String localFilename, String localFilePath)
            throws Exception {
        MinioClient minioClient = null;
        boolean success = false;
        try {
            minioClient =
                    MinioClient.builder()
                            .endpoint(storageUrl)
                            .credentials(accessKey, secretKey)
                            .build();

            boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket(uploadBucketName).build());
            if (!found) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(uploadBucketName).build());
            } else {
                log.warn("Bucket {} already exists.", uploadBucketName);
            }
            log.info("Uploading file: {} to bucket: {}", uploadFilename, uploadBucketName);
            String localFile = localFilePath + localFilename;
            checkIfFileExistInLocal(localFile);
            Map<String, String> updateMetDataProperty = new HashMap<>(metaDataProperty);
            updateMetDataProperty.put("original_filename", localFilename);

            minioClient.uploadObject(
                    UploadObjectArgs.builder()
                            .bucket(uploadBucketName)
                            .object(uploadFilename)
                            .filename(localFile)
                            .userMetadata(updateMetDataProperty)
                            .build());
            log.info("'{}' is successfully uploaded as object '{}' to bucket '{}'.", localFile, uploadFilename, uploadBucketName);
            success = true;
        } catch (MinioException e) {
            log.error("Minio-client error occurred: ", e);
            log.error("HTTP trace: {}", e.httpTrace());
        } catch (Exception e) {
            log.error("Unknown Error occurred: ", e);
        } finally {
            if (minioClient != null) {
                log.info("Closing the minio client.");
                minioClient.close();
            }
        }
        return success;
    }

    private static void checkIfFileExistInLocal(String localFile) throws FileNotFoundException {
        Path pathOfLocalFile = Path.of(localFile);
        if (!Files.exists(pathOfLocalFile, LinkOption.NOFOLLOW_LINKS) && !Files.isRegularFile(pathOfLocalFile)) {
            log.error("Local file {} does not exist.", localFile);
            throw new FileNotFoundException("Local file " + localFile + " does not exist.");
        }
    }
}
