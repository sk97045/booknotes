# Design Youtube

## 1. Requirements (~5 min)

![data-tables](images/hello-interview/1.png)

## 2. Core Entities (~5 min)

For the video metadata, we are assuming an upload rate of ~1M videos/day. This means, over the course of the year, we'll have ~365M records in the database representing videos. As a result, we should consider storing video metadata in a database that can be horizontally partitioned, such as Cassandra. **Cassandra** offers high availability and enables us to choose a partition key for our data. 

We can partition on the videoId, since we aren't really worried about bulk-accessing videos in this design, just querying individual videos.


## 3. API / Interface (~5 min)

The API is the primary interface that users will interact with. It's important to define the API early on, as it will guide your high-level design. We just need to define an endpoint for each of our functional requirements.

Let's start with an endpoint to upload a video. We might have an endpoint like this:

```
POST /upload
Request:
{
  Video,
  VideoMetadata
}
```

To stream a video, our endpoint might retrieve the video data to play it on device:
```
GET /videos/{videoId} -> Video & VideoMetadata
```



```
{
  "videoId": "vid_123",
  "uploaderId": "user_42",
  "title": "My hiking video",
  "uploadId": "s3-multipart-upload-id",
  "uploadStatus": "UPLOADING",
  "thumbnailUrl": "https://cdn.example.com/videos/vid_123/thumb.jpg",
  "manifestUrl": "https://cdn.example.com/videos/vid_123/master.m3u8",
  "transcriptUrl": "https://cdn.example.com/videos/vid_123/transcript.vtt",
  "createdAt": "2026-07-20T10:00:00Z"
  "chunks": [
    {
      "partNumber": 1,
      "fingerprint": "sha256:ab12cd34...",
      "status": "Uploaded",
      "etag": "\"9b2cf...\""
    },
    {
      "partNumber": 2,
      "fingerprint": "sha256:ef56gh78...",
      "status": "NotUploaded"
    }
  ]
}
```

---

## 4. High-Level Design (~10–15 min)

### Content of Manifest file

```
#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=800000,RESOLUTION=640x360
360p/playlist.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=2500000,RESOLUTION=1280x720
720p/playlist.m3u8
#EXT-X-STREAM-INF:BANDWIDTH=5000000,RESOLUTION=1920x1080
1080p/playlist.m3u8
```

And the 720p example file

```
#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:4
#EXT-X-MEDIA-SEQUENCE:0

#EXTINF:4.0,
segments/720p/segment-0001.ts
#EXTINF:4.0,
segments/720p/segment-0002.ts
#EXTINF:4.0,
segments/720p/segment-0003.ts
#EXTINF:4.0,
segments/720p/segment-0004.ts
#EXT-X-ENDLIST
```

![data-tables](images/hello-interview/2.png)