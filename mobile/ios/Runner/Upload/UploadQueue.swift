import SQLiteData
import StructuredFieldValues

class UploadQueue {
  private static let structuredEncoder = StructuredFieldValueEncoder()
  private static var queueProcessingTask: Task<Void, Never>?
  private static var queueProcessingLock = NSLock()

  private let db: DatabasePool
  private let cellularSession: URLSession
  private let wifiOnlySession: URLSession
  private let flutterApi: UploadFlutterApi

  init(db: DatabasePool, cellularSession: URLSession, wifiOnlySession: URLSession, flutterApi: UploadFlutterApi) {
    self.db = db
    self.cellularSession = cellularSession
    self.wifiOnlySession = wifiOnlySession
    self.flutterApi = flutterApi
  }

  func startQueueProcessing() {
    dPrint("Starting upload queue processing")
    Self.queueProcessingLock.withLock {
      guard Self.queueProcessingTask == nil else { return }
      Self.queueProcessingTask = Task {
        await startUploads()
        Self.queueProcessingLock.withLock { Self.queueProcessingTask = nil }
      }
    }
  }

  private func startUploads() async {
    dPrint("Processing download queue")
    guard NetworkMonitor.shared.isConnected,
      let backupEnabled = try? await db.read({ conn in try Store.get(conn, StoreKey.enableBackup) }),
      backupEnabled
    else { return dPrint("Download queue paused: network disconnected or backup disabled") }

    do {
      let tasks = try await db.read({ conn in
        try UploadTask.assetData
          .where { task, _ in
            task.status.eq(TaskStatus.uploadPending) && task.attempts < TaskConfig.maxAttempts
              && task.filePath.isNot(nil)
          }
          .limit { task, _ in UploadTaskStat.availableUploadSlots }
          .order { task, asset in (task.priority.desc(), task.createdAt) }
          .fetchAll(conn)
      })
      if tasks.isEmpty { return dPrint("No upload tasks to process") }

      await withTaskGroup(of: Void.self) { group in
        for task in tasks {
          group.addTask { await self.startUpload(multipart: task) }
        }
      }
    } catch {
      dPrint("Upload queue error: \(error)")
    }
  }

  private func startUpload(multipart task: LocalAssetTaskData) async {
    dPrint("Uploading asset resource \(task.localId) of type \(task.type.rawValue)")
    defer { startQueueProcessing() }

    guard let filePath = task.filePath else {
      dPrint("Upload failed for \(task.taskId), file path is nil")
      return handleFailure(task: task, code: .fileNotFound)
    }

    let (url, accessToken, session): (URL, String, URLSession)
    do {
      (url, accessToken, session) = try await db.read { conn in
        guard let url = try Store.get(conn, StoreKey.serverEndpoint),
          let accessToken = try Store.get(conn, StoreKey.accessToken)
        else {
          throw StoreError.notFound
        }

        let session =
          switch task.type {
          case .image:
            (try? Store.get(conn, StoreKey.useWifiForUploadPhotos)) ?? false ? cellularSession : wifiOnlySession
          case .video:
            (try? Store.get(conn, StoreKey.useWifiForUploadVideos)) ?? false ? cellularSession : wifiOnlySession
          default: wifiOnlySession
          }
        return (url, accessToken, session)
      }
    } catch {
      dPrint("Upload failed for \(task.taskId), could not retrieve server URL or access token: \(error)")
      return handleFailure(task: task, code: .noServerUrl)
    }

    var request = URLRequest(url: url.appendingPathComponent("/assets"))
    request.httpMethod = "POST"
    request.setValue(accessToken, forHTTPHeaderField: UploadHeaders.userToken.rawValue)
    request.setValue(AssetData.contentType, forHTTPHeaderField: "Content-Type")

    let sessionTask = session.uploadTask(with: request, fromFile: filePath)
    sessionTask.taskDescription = String(task.taskId)
    sessionTask.priority = task.priority
    do {
      try? FileManager.default.removeItem(at: filePath)  // upload task already copied the file
      try await db.write { conn in
        try UploadTask.update { row in
          row.status = .uploadQueued
          row.filePath = nil
        }
        .where { $0.id.eq(task.taskId) }
        .execute(conn)
      }

      sessionTask.resume()
      dPrint("Upload started for task \(task.taskId) using \(session == wifiOnlySession ? "WiFi" : "Cellular") session")
    } catch {
      dPrint("Upload failed for \(task.taskId), could not update queue status: \(error.localizedDescription)")
    }
  }

  private func handleFailure(task: LocalAssetTaskData, code: UploadErrorCode) {
    do {
      try db.write { conn in
        try UploadTask.retryOrFail(code: code, status: .uploadFailed).where { $0.id.eq(task.taskId) }.execute(conn)
      }
    } catch {
      dPrint("Failed to update upload failure status for task \(task.taskId): \(error)")
    }
  }
}
