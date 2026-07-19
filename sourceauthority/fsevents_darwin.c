#include "fsevents_darwin.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <unistd.h>

struct fk_fsevents_stream {
  FSEventStreamRef stream;
  dispatch_queue_t queue;
  uintptr_t handle;
  bool started;
  int pinned_root_fd;
  int pinned_watch_fd;
};

static void fk_set_error(char **output, const char *message) {
  if (output != NULL) {
    *output = strdup(message);
  }
}

static void fk_fsevents_callback(ConstFSEventStreamRef stream_ref,
                                 void *callback_info, size_t count,
                                 void *event_paths,
                                 const FSEventStreamEventFlags flags[],
                                 const FSEventStreamEventId ids[]) {
  (void)stream_ref;
  fk_fsevents_stream *stream = callback_info;
  goFuseKitFSEventsCallback(stream->handle, count, (char **)event_paths,
                           (uint32_t *)flags, (uint64_t *)ids);
}

static int fk_open_components(const char *path, bool parent_only,
                              char **leaf, char **error_message) {
  if (path == NULL || path[0] != '/') {
    fk_set_error(error_message, "source root is not absolute");
    return -1;
  }
  char *copy = strdup(path + 1);
  if (copy == NULL) {
    fk_set_error(error_message, "allocate source root traversal");
    return -1;
  }
  int fd = open("/", O_EVTONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) {
    free(copy);
    fk_set_error(error_message, strerror(errno));
    return -1;
  }
  char *save = NULL;
  char *component = strtok_r(copy, "/", &save);
  while (component != NULL) {
    char *next = strtok_r(NULL, "/", &save);
    if (parent_only && next == NULL) {
      *leaf = strdup(component);
      if (*leaf == NULL) {
        close(fd);
        free(copy);
        fk_set_error(error_message, "allocate source root leaf");
        return -1;
      }
      free(copy);
      return fd;
    }
    int opened = openat(fd, component,
                        O_EVTONLY | O_NOFOLLOW | O_CLOEXEC |
                            (next != NULL ? O_DIRECTORY : 0));
    if (opened < 0) {
      close(fd);
      free(copy);
      fk_set_error(error_message, strerror(errno));
      return -1;
    }
    struct stat status;
    if (fstat(opened, &status) != 0 ||
        (next != NULL && !S_ISDIR(status.st_mode))) {
      close(opened);
      close(fd);
      free(copy);
      fk_set_error(error_message, "source root ancestor is not a directory");
      return -1;
    }
    close(fd);
    fd = opened;
    component = next;
  }
  if (parent_only) {
    close(fd);
    free(copy);
    fk_set_error(error_message, "volume root cannot be an exact-file root");
    return -1;
  }
  free(copy);
  return fd;
}

static int fk_device_relative_path(int fd, const char *mount_path,
                                   char **output, char **error_message) {
  char resolved[PATH_MAX];
  if (fcntl(fd, F_GETPATH, resolved) != 0) {
    fk_set_error(error_message, strerror(errno));
    return 0;
  }
  size_t mount_length = strlen(mount_path);
  if (strncmp(resolved, mount_path, mount_length) != 0 ||
      (mount_length > 1 && resolved[mount_length] != '\0' &&
       resolved[mount_length] != '/')) {
    fk_set_error(error_message, "pinned source root escaped its mounted volume");
    return 0;
  }
  const char *relative = resolved + mount_length;
  while (*relative == '/') {
    relative++;
  }
  *output = strdup(relative);
  if (*output == NULL) {
    fk_set_error(error_message, "allocate device-relative source path");
    return 0;
  }
  return 1;
}

void fk_fsevents_close_pins(int pinned_root_fd, int pinned_watch_fd) {
  if (pinned_root_fd >= 0) {
    close(pinned_root_fd);
  }
  if (pinned_watch_fd >= 0 && pinned_watch_fd != pinned_root_fd) {
    close(pinned_watch_fd);
  }
}

int fk_fsevents_fd_volume_uuid(int fd, char **volume_uuid,
                              char **error_message) {
  struct stat status;
  if (fstat(fd, &status) != 0) {
    fk_set_error(error_message, strerror(errno));
    return 0;
  }
  CFUUIDRef uuid = FSEventsCopyUUIDForDevice(status.st_dev);
  if (uuid == NULL) {
    fk_set_error(error_message, "resolve FSEvents volume UUID");
    return 0;
  }
  CFStringRef uuid_string = CFUUIDCreateString(kCFAllocatorDefault, uuid);
  CFRelease(uuid);
  if (uuid_string == NULL) {
    fk_set_error(error_message, "encode FSEvents volume UUID");
    return 0;
  }
  CFIndex maximum = CFStringGetMaximumSizeForEncoding(
                        CFStringGetLength(uuid_string), kCFStringEncodingUTF8) +
                    1;
  char *uuid_copy = malloc((size_t)maximum);
  if (uuid_copy == NULL ||
      !CFStringGetCString(uuid_string, uuid_copy, maximum,
                          kCFStringEncodingUTF8)) {
    free(uuid_copy);
    CFRelease(uuid_string);
    fk_set_error(error_message, "copy FSEvents volume UUID");
    return 0;
  }
  CFRelease(uuid_string);
  *volume_uuid = uuid_copy;
  return 1;
}

int fk_fsevents_root_info(const char *path, int root_kind, dev_t *device,
                          int *pinned_root_fd, int *pinned_watch_fd,
                          char **watch_device_path, char **event_device_path,
                          char **volume_uuid, uint64_t *inode,
                          int64_t *birthtime_sec, int64_t *birthtime_nsec,
                          uint64_t *current_event_id, char **error_message) {
  int root_fd = -1;
  int watch_fd = -1;
  char *leaf = NULL;
  if (root_kind == 1) {
    watch_fd = fk_open_components(path, true, &leaf, error_message);
    if (watch_fd < 0) {
      return 0;
    }
    root_fd = openat(watch_fd, leaf, O_EVTONLY | O_NOFOLLOW | O_CLOEXEC);
    free(leaf);
    if (root_fd < 0) {
      fk_fsevents_close_pins(root_fd, watch_fd);
      fk_set_error(error_message, strerror(errno));
      return 0;
    }
  } else if (root_kind == 2) {
    root_fd = fk_open_components(path, false, &leaf, error_message);
    if (root_fd < 0) {
      return 0;
    }
    watch_fd = root_fd;
  } else {
    fk_set_error(error_message, "invalid source root kind");
    return 0;
  }

  struct stat root_status;
  struct stat watch_status;
  struct statfs filesystem;
  if (fstat(root_fd, &root_status) != 0 || fstat(watch_fd, &watch_status) != 0 ||
      fstatfs(watch_fd, &filesystem) != 0) {
    fk_fsevents_close_pins(root_fd, watch_fd);
    fk_set_error(error_message, strerror(errno));
    return 0;
  }
  if ((root_kind == 1 && !S_ISREG(root_status.st_mode)) ||
      (root_kind == 2 && !S_ISDIR(root_status.st_mode)) ||
      !S_ISDIR(watch_status.st_mode) || root_status.st_dev != watch_status.st_dev) {
    fk_fsevents_close_pins(root_fd, watch_fd);
    fk_set_error(error_message, "source root kind or device changed while pinning");
    return 0;
  }
  char *watch_path = NULL;
  char *event_path = NULL;
  if (!fk_device_relative_path(watch_fd, filesystem.f_mntonname, &watch_path,
                               error_message) ||
      !fk_device_relative_path(root_fd, filesystem.f_mntonname, &event_path,
                               error_message)) {
    free(watch_path);
    free(event_path);
    fk_fsevents_close_pins(root_fd, watch_fd);
    return 0;
  }

  CFUUIDRef uuid = FSEventsCopyUUIDForDevice(watch_status.st_dev);
  if (uuid == NULL) {
    free(watch_path);
    free(event_path);
    fk_fsevents_close_pins(root_fd, watch_fd);
    fk_set_error(error_message, "resolve FSEvents volume UUID");
    return 0;
  }
  CFStringRef uuid_string = CFUUIDCreateString(kCFAllocatorDefault, uuid);
  CFRelease(uuid);
  if (uuid_string == NULL) {
    free(watch_path);
    free(event_path);
    fk_fsevents_close_pins(root_fd, watch_fd);
    fk_set_error(error_message, "encode FSEvents volume UUID");
    return 0;
  }
  CFIndex maximum = CFStringGetMaximumSizeForEncoding(
                        CFStringGetLength(uuid_string), kCFStringEncodingUTF8) +
                    1;
  char *uuid_copy = malloc((size_t)maximum);
  if (uuid_copy == NULL ||
      !CFStringGetCString(uuid_string, uuid_copy, maximum,
                          kCFStringEncodingUTF8)) {
    free(watch_path);
    free(event_path);
    free(uuid_copy);
    CFRelease(uuid_string);
    fk_fsevents_close_pins(root_fd, watch_fd);
    fk_set_error(error_message, "copy FSEvents volume UUID");
    return 0;
  }
  CFRelease(uuid_string);

  *device = watch_status.st_dev;
  *pinned_root_fd = root_fd;
  *pinned_watch_fd = watch_fd == root_fd ? -1 : watch_fd;
  *watch_device_path = watch_path;
  *event_device_path = event_path;
  *volume_uuid = uuid_copy;
  *inode = watch_status.st_ino;
  *birthtime_sec = watch_status.st_birthtimespec.tv_sec;
  *birthtime_nsec = watch_status.st_birthtimespec.tv_nsec;
  *current_event_id = FSEventsGetLastEventIdForDeviceBeforeTime(
      watch_status.st_dev, CFAbsoluteTimeGetCurrent());
  return 1;
}

fk_fsevents_stream *fk_fsevents_open(dev_t device, int pinned_root_fd,
                                    int pinned_watch_fd,
                                    const char *watch_device_path,
                                    uint64_t since_when, uintptr_t handle) {
  fk_fsevents_stream *result = calloc(1, sizeof(*result));
  if (result == NULL) {
    fk_fsevents_close_pins(pinned_root_fd, pinned_watch_fd);
    return NULL;
  }
  result->handle = handle;
  result->pinned_root_fd = pinned_root_fd;
  result->pinned_watch_fd = pinned_watch_fd;
  CFStringRef path = CFStringCreateWithFileSystemRepresentation(
      kCFAllocatorDefault, watch_device_path);
  if (path == NULL) {
    fk_fsevents_close_pins(pinned_root_fd, pinned_watch_fd);
    free(result);
    return NULL;
  }
  const void *values[] = {path};
  CFArrayRef paths = CFArrayCreate(kCFAllocatorDefault, values, 1,
                                   &kCFTypeArrayCallBacks);
  CFRelease(path);
  if (paths == NULL) {
    fk_fsevents_close_pins(pinned_root_fd, pinned_watch_fd);
    free(result);
    return NULL;
  }
  FSEventStreamContext context = {0, result, NULL, NULL, NULL};
  FSEventStreamCreateFlags create_flags =
      kFSEventStreamCreateFlagWatchRoot |
      kFSEventStreamCreateFlagFileEvents |
      kFSEventStreamCreateFlagFullHistory;
  result->stream = FSEventStreamCreateRelativeToDevice(
      kCFAllocatorDefault, fk_fsevents_callback, &context, device, paths,
      since_when, 0.1, create_flags);
  CFRelease(paths);
  if (result->stream == NULL) {
    fk_fsevents_close_pins(pinned_root_fd, pinned_watch_fd);
    free(result);
    return NULL;
  }
  result->queue = dispatch_queue_create(
      "com.yasyf.fusekit.sourceauthority.fsevents", DISPATCH_QUEUE_SERIAL);
  if (result->queue == NULL) {
    FSEventStreamRelease(result->stream);
    fk_fsevents_close_pins(pinned_root_fd, pinned_watch_fd);
    free(result);
    return NULL;
  }
  FSEventStreamSetDispatchQueue(result->stream, result->queue);
  return result;
}

int fk_fsevents_start(fk_fsevents_stream *stream) {
  if (stream == NULL || stream->started) {
    return stream != NULL;
  }
  if (!FSEventStreamStart(stream->stream)) {
    return 0;
  }
  stream->started = true;
  return 1;
}

void fk_fsevents_flush(fk_fsevents_stream *stream) {
  if (stream != NULL && stream->started) {
    FSEventStreamFlushSync(stream->stream);
  }
}

static void fk_fsevents_queue_barrier(void *unused) { (void)unused; }

void fk_fsevents_close(fk_fsevents_stream *stream) {
  if (stream == NULL) {
    return;
  }
  if (stream->started) {
    FSEventStreamStop(stream->stream);
    stream->started = false;
  }
  FSEventStreamInvalidate(stream->stream);
  dispatch_sync_f(stream->queue, NULL, fk_fsevents_queue_barrier);
  FSEventStreamRelease(stream->stream);
  fk_fsevents_close_pins(stream->pinned_root_fd, stream->pinned_watch_fd);
#if !OS_OBJECT_USE_OBJC
  dispatch_release(stream->queue);
#endif
  free(stream);
}

void fk_fsevents_free(void *value) { free(value); }
