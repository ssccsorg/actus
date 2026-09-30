// Process helpers shared by the kinds that spawn one.
//
// A kind that drives an external binary holds a child process per turn, and it
// is usually a launcher with children of its own, so stopping a turn means
// signalling the process group as well as the direct child. A straggler left
// holding the output pipes would otherwise delay what the turn records, and on
// the exit path it would outlive the server.

use tokio::process::Child;

/// Kill a child and its process group, then reap it. The group signal goes out
/// before the reap, while the group is known to exist, and the direct child is
/// signalled anyway as the fallback for a platform or a spawn without a group.
pub async fn kill_child_group(child: &mut Child) {
    if let Some(pid) = child.id() {
        #[cfg(unix)]
        unsafe {
            libc::kill(-(pid as i32), libc::SIGKILL);
        }
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
}
