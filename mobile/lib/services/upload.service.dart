import 'dart:async';

import 'package:hooks_riverpod/hooks_riverpod.dart';
import 'package:immich_mobile/domain/models/asset/base_asset.model.dart';
import 'package:immich_mobile/infrastructure/repositories/backup.repository.dart';
import 'package:immich_mobile/platform/upload_api.g.dart';
import 'package:immich_mobile/providers/infrastructure/platform.provider.dart';

final uploadServiceProvider = Provider((ref) {
  final service = UploadService(ref.watch(backupRepositoryProvider));

  ref.onDispose(service.dispose);
  return service;
});

class UploadService extends UploadFlutterApi {
  UploadService(this._backupRepository);

  final DriftBackupRepository _backupRepository;

  final StreamController<UploadApiTaskStatus> _taskStatusController = StreamController<UploadApiTaskStatus>.broadcast();
  final StreamController<UploadApiTaskProgress> _taskProgressController =
      StreamController<UploadApiTaskProgress>.broadcast();

  Stream<UploadApiTaskStatus> get taskStatusStream => _taskStatusController.stream;
  Stream<UploadApiTaskProgress> get taskProgressStream => _taskProgressController.stream;

  bool shouldAbortQueuingTasks = false;

  @override
  void onTaskProgress(UploadApiTaskProgress update) {
    if (!_taskProgressController.isClosed) {
      _taskProgressController.add(update);
    }
  }

  @override
  void onTaskStatus(UploadApiTaskStatus update) {
    if (!_taskStatusController.isClosed) {
      _taskStatusController.add(update);
    }
  }

  void dispose() {
    _taskStatusController.close();
    _taskProgressController.close();
  }

  Future<({int total, int remainder, int processing})> getBackupCounts(String userId) {
    return _backupRepository.getAllCounts(userId);
  }

  Future<void> manualBackup(List<LocalAsset> localAssets) {
    return uploadApi.enqueue(localAssets.map((e) => e.id).toList(growable: false));
  }

  /// Find backup candidates
  /// Build the upload tasks
  /// Enqueue the tasks
  Future<void> startBackup() async {
    return uploadApi.refresh();
  }

  /// Cancel all ongoing uploads and reset the upload queue
  ///
  /// Return the number of left over tasks in the queue
  Future<void> cancelBackup() {
    return uploadApi.cancelAll();
  }
}
