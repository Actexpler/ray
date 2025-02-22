package io.ray.runtime;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import io.ray.api.BaseActorHandle;
import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import io.ray.api.id.UniqueId;
import io.ray.api.runtimecontext.ResourceValue;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.context.NativeWorkerContext;
import io.ray.runtime.exception.RayIntentionalSystemExitException;
import io.ray.runtime.gcs.GcsClient;
import io.ray.runtime.gcs.GcsClientOptions;
import io.ray.runtime.generated.Common.RuntimeEnv;
import io.ray.runtime.generated.Common.WorkerType;
import io.ray.runtime.generated.Gcs.GcsNodeInfo;
import io.ray.runtime.generated.Gcs.JobConfig;
import io.ray.runtime.object.NativeObjectStore;
import io.ray.runtime.runner.RunManager;
import io.ray.runtime.task.NativeTaskExecutor;
import io.ray.runtime.task.NativeTaskSubmitter;
import io.ray.runtime.task.TaskExecutor;
import io.ray.runtime.util.BinaryFileUtil;
import io.ray.runtime.util.JniUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Native runtime for cluster mode. */
public final class RayNativeRuntime extends AbstractRayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayNativeRuntime.class);

  private boolean startRayHead = false;

  /**
   * In Java, GC runs in a standalone thread, and we can't control the exact timing of garbage
   * collection. By using this lock, when {@link NativeObjectStore#nativeRemoveLocalReference} is
   * executing, the core worker will not be shut down, therefore it guarantees some kind of
   * thread-safety. Note that this guarantee only works for driver.
   */
  private final ReadWriteLock shutdownLock = new ReentrantReadWriteLock();

  public RayNativeRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

  private void updateSessionDir(GcsClient gcsClient) {
    // Fetch session dir from GCS.
    final String sessionDir = gcsClient.getInternalKV("@:session:session_dir");
    Preconditions.checkNotNull(sessionDir);
    rayConfig.setSessionDir(sessionDir);
  }

  @Override
  public void start() {
    try {
      if (rayConfig.workerMode == WorkerType.DRIVER && rayConfig.getRedisAddress() == null) {
        // Set it to true before `RunManager.startRayHead` so `Ray.shutdown()` can still kill
        // Ray processes even if `Ray.init()` failed.
        startRayHead = true;
        RunManager.startRayHead(rayConfig);
      }
      Preconditions.checkNotNull(rayConfig.getRedisAddress());

      // In order to remove redis dependency in Java lang, we use a temp dir to load library
      // instead of getting session dir from redis.
      if (rayConfig.workerMode == WorkerType.DRIVER) {
        String tmpDir = "/tmp/ray/".concat(String.valueOf(System.currentTimeMillis()));
        JniUtils.loadLibrary(tmpDir, BinaryFileUtil.CORE_WORKER_JAVA_LIBRARY, true);
        gcsClient = new GcsClient(rayConfig.getRedisAddress(), rayConfig.redisPassword);
        updateSessionDir(gcsClient);
        Preconditions.checkNotNull(rayConfig.sessionDir);
      } else {
        // Expose ray ABI symbols which may be depended by other shared
        // libraries such as libstreaming_java.so.
        // See BUILD.bazel:libcore_worker_library_java.so
        Preconditions.checkNotNull(rayConfig.sessionDir);
        JniUtils.loadLibrary(rayConfig.sessionDir, BinaryFileUtil.CORE_WORKER_JAVA_LIBRARY, true);
        gcsClient = new GcsClient(rayConfig.getRedisAddress(), rayConfig.redisPassword);
      }

      if (rayConfig.workerMode == WorkerType.DRIVER) {
        GcsNodeInfo nodeInfo = gcsClient.getNodeToConnectForDriver(rayConfig.nodeIp);
        rayConfig.rayletSocketName = nodeInfo.getRayletSocketName();
        rayConfig.objectStoreSocketName = nodeInfo.getObjectStoreSocketName();
        rayConfig.nodeManagerPort = nodeInfo.getNodeManagerPort();
      }

      if (rayConfig.getJobId() == JobId.NIL) {
        rayConfig.setJobId(gcsClient.nextJobId());
      }
      int numWorkersPerProcess =
          rayConfig.workerMode == WorkerType.DRIVER ? 1 : rayConfig.numWorkersPerProcess;

      byte[] serializedJobConfig = null;
      if (rayConfig.workerMode == WorkerType.DRIVER) {
        JobConfig.Builder jobConfigBuilder =
            JobConfig.newBuilder()
                .setNumJavaWorkersPerProcess(rayConfig.numWorkersPerProcess)
                .addAllJvmOptions(rayConfig.jvmOptionsForJavaWorker)
                .addAllCodeSearchPath(rayConfig.codeSearchPath)
                .setRayNamespace(rayConfig.namespace);
        RuntimeEnv.Builder runtimeEnvBuilder = RuntimeEnv.newBuilder();
        if (!rayConfig.workerEnv.isEmpty()) {
          // TODO(SongGuyang): Suppport complete runtime env interface for users.
          // Set worker env to the serialized runtime env json.
          Gson gson = new Gson();
          Map<String, Map<String, String>> runtimeEnv = new HashMap<>();
          runtimeEnv.put("env_vars", rayConfig.workerEnv);
          String gsonString = gson.toJson(runtimeEnv);
          runtimeEnvBuilder.setSerializedRuntimeEnv(gsonString);
        } else {
          runtimeEnvBuilder.setSerializedRuntimeEnv("{}");
        }
        jobConfigBuilder.setRuntimeEnv(runtimeEnvBuilder.build());
        serializedJobConfig = jobConfigBuilder.build().toByteArray();
      }

      nativeInitialize(
          rayConfig.workerMode.getNumber(),
          rayConfig.nodeIp,
          rayConfig.getNodeManagerPort(),
          rayConfig.workerMode == WorkerType.DRIVER ? System.getProperty("user.dir") : "",
          rayConfig.objectStoreSocketName,
          rayConfig.rayletSocketName,
          (rayConfig.workerMode == WorkerType.DRIVER ? rayConfig.getJobId() : JobId.NIL).getBytes(),
          new GcsClientOptions(rayConfig),
          numWorkersPerProcess,
          rayConfig.logDir,
          serializedJobConfig,
          rayConfig.getStartupToken());

      taskExecutor = new NativeTaskExecutor(this);
      workerContext = new NativeWorkerContext();
      objectStore = new NativeObjectStore(workerContext, shutdownLock);
      taskSubmitter = new NativeTaskSubmitter();

      LOGGER.debug(
          "RayNativeRuntime started with store {}, raylet {}",
          rayConfig.objectStoreSocketName,
          rayConfig.rayletSocketName);
    } catch (Exception e) {
      if (startRayHead) {
        try {
          RunManager.stopRay();
        } catch (Exception e2) {
          // Ignore
        }
      }
      throw e;
    }
  }

  @Override
  public void shutdown() {
    // `shutdown` won't be called concurrently, but the lock is also used in `NativeObjectStore`.
    // When an object is garbage collected, the object will be unregistered from core worker.
    // Since GC runs in a separate thread, we need to make sure that core worker is available
    // when `NativeObjectStore` is accessing core worker in the GC thread.
    Lock writeLock = shutdownLock.writeLock();
    writeLock.lock();
    try {
      if (rayConfig.workerMode == WorkerType.DRIVER) {
        nativeShutdown();
        if (startRayHead) {
          startRayHead = false;
          RunManager.stopRay();
        }
      }
      if (null != gcsClient) {
        gcsClient.destroy();
        gcsClient = null;
      }
      LOGGER.debug("RayNativeRuntime shutdown");
    } finally {
      writeLock.unlock();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends BaseActorHandle> Optional<T> getActor(String name, boolean global) {
    if (name.isEmpty()) {
      return Optional.empty();
    }
    byte[] actorIdBytes = nativeGetActorIdOfNamedActor(name, global);
    ActorId actorId = ActorId.fromBytes(actorIdBytes);
    if (actorId.isNil()) {
      return Optional.empty();
    } else {
      return Optional.of((T) getActorHandle(actorId));
    }
  }

  @Override
  public void killActor(BaseActorHandle actor, boolean noRestart) {
    nativeKillActor(actor.getId().getBytes(), noRestart);
  }

  @Override
  public Object getAsyncContext() {
    return new AsyncContext(
        workerContext.getCurrentWorkerId(), workerContext.getCurrentClassLoader());
  }

  @Override
  public void setAsyncContext(Object asyncContext) {
    nativeSetCoreWorker(((AsyncContext) asyncContext).workerId.getBytes());
    workerContext.setCurrentClassLoader(((AsyncContext) asyncContext).currentClassLoader);
    super.setAsyncContext(asyncContext);
  }

  @Override
  public void exitActor() {
    if (rayConfig.workerMode != WorkerType.WORKER || runtimeContext.getCurrentActorId().isNil()) {
      throw new RuntimeException("This shouldn't be called on a non-actor worker.");
    }
    LOGGER.info("Actor {} is exiting.", runtimeContext.getCurrentActorId());
    throw new RayIntentionalSystemExitException(
        String.format("Actor %s is exiting.", runtimeContext.getCurrentActorId()));
  }

  @Override
  public void run() {
    Preconditions.checkState(rayConfig.workerMode == WorkerType.WORKER);
    nativeRunTaskExecutor(taskExecutor);
  }

  @Override
  public Map<String, List<ResourceValue>> getAvailableResourceIds() {
    return nativeGetResourceIds();
  }

  @Override
  public String getNamespace() {
    return nativeGetNamespace();
  }

  private static native void nativeInitialize(
      int workerMode,
      String ndoeIpAddress,
      int nodeManagerPort,
      String driverName,
      String storeSocket,
      String rayletSocket,
      byte[] jobId,
      GcsClientOptions gcsClientOptions,
      int numWorkersPerProcess,
      String logDir,
      byte[] serializedJobConfig,
      int startupToken);

  private static native void nativeRunTaskExecutor(TaskExecutor taskExecutor);

  private static native void nativeShutdown();

  private static native void nativeKillActor(byte[] actorId, boolean noRestart);

  private static native byte[] nativeGetActorIdOfNamedActor(String actorName, boolean global);

  private static native void nativeSetCoreWorker(byte[] workerId);

  private static native Map<String, List<ResourceValue>> nativeGetResourceIds();

  private static native String nativeGetNamespace();

  static class AsyncContext {

    public final UniqueId workerId;
    public final ClassLoader currentClassLoader;

    AsyncContext(UniqueId workerId, ClassLoader currentClassLoader) {
      this.workerId = workerId;
      this.currentClassLoader = currentClassLoader;
    }
  }
}
