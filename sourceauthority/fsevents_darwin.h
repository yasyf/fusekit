#ifndef FUSEKIT_SOURCEAUTHORITY_FSEVENTS_DARWIN_H
#define FUSEKIT_SOURCEAUTHORITY_FSEVENTS_DARWIN_H

#include <CoreServices/CoreServices.h>
#include <dispatch/dispatch.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

typedef struct fk_fsevents_stream fk_fsevents_stream;

int fk_fsevents_root_info(const char *path, int root_kind, dev_t *device,
                          int *pinned_root_fd, int *pinned_watch_fd,
                          char **watch_device_path, char **event_device_path,
                          char **volume_uuid, uint64_t *inode,
                          int64_t *birthtime_sec, int64_t *birthtime_nsec,
                          uint64_t *current_event_id, char **error_message);
int fk_fsevents_fd_volume_uuid(int fd, char **volume_uuid,
                              char **error_message);
fk_fsevents_stream *fk_fsevents_open(dev_t device, int pinned_root_fd,
                                    int pinned_watch_fd,
                                    const char *watch_device_path,
                                    uint64_t since_when, uintptr_t handle);
void fk_fsevents_close_pins(int pinned_root_fd, int pinned_watch_fd);
int fk_fsevents_start(fk_fsevents_stream *stream);
void fk_fsevents_flush(fk_fsevents_stream *stream);
void fk_fsevents_close(fk_fsevents_stream *stream);
void fk_fsevents_free(void *value);

extern int goFuseKitFSEventsCallback(uintptr_t handle, size_t count,
                                    char **paths, uint32_t *flags,
                                    uint64_t *ids);

#endif
