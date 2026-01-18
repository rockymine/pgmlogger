package com.github.rockymine.pgmlogger.webhook;

import com.google.gson.Gson;
import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class DiscordWebhookClient {

  private final Logger logger;
  private final Gson gson = new Gson();
  private final OkHttpClient httpClient = new OkHttpClient.Builder()
      .connectTimeout(10, TimeUnit.SECONDS)
      .readTimeout(15, TimeUnit.SECONDS)
      .writeTimeout(15, TimeUnit.SECONDS)
      .build();

  public DiscordWebhookClient(Logger logger) {
    this.logger = logger;
  }

  public void sendMatchSaved(
      String webhookUrl,
      String matchName,
      String matchId,
      String duration,
      File parquetFile,
      String fileSize) {
    if (webhookUrl == null || webhookUrl.trim().isEmpty()) {
      return;
    }
    if (parquetFile == null || !parquetFile.exists()) {
      logger.warning("Webhook skipped: parquet file is missing.");
      return;
    }

    String content = buildContent(matchName, matchId, duration, fileSize);
    String payloadJson = gson.toJson(new Payload(content));
    String boundary = "------------------------" + UUID.randomUUID().toString().replace("-", "");
    MediaType jsonType = MediaType.parse("application/json; charset=utf-8");
    MediaType fileType = MediaType.parse("application/octet-stream");

    RequestBody payloadBody = RequestBody.create(jsonType, payloadJson);
    RequestBody fileBody = RequestBody.create(fileType, parquetFile);
    RequestBody multipartBody = new MultipartBody.Builder(boundary)
        .setType(MultipartBody.FORM)
        .addFormDataPart("payload_json", null, payloadBody)
        .addFormDataPart("file", parquetFile.getName(), fileBody)
        .build();

    Request request = new Request.Builder().url(webhookUrl).post(multipartBody).build();

    try {
      try (Response response = httpClient.newCall(request).execute()) {
        if (!response.isSuccessful()) {
          logger.warning("Discord webhook returned HTTP " + response.code());
        }
      }
    } catch (IOException e) {
      logger.log(Level.WARNING, "Failed to send Discord webhook", e);
    }
  }

  private String buildContent(String matchName, String matchId, String duration, String fileSize) {
    return "**\\#" + safe(matchId) + "**" + " - `"
        + safe(matchName) + "`" + " - `"
        + safe(duration) + "`\n"
        + "File Size: `" + safe(fileSize) + "`";
  }

  private String safe(String value) {
    return value == null ? "unknown" : value;
  }

  private static class Payload {
    private final String content;

    private Payload(String content) {
      this.content = content;
    }
  }
}
