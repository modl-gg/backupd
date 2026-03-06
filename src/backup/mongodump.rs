use anyhow::{Context, Result};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, ChildStderr, ChildStdout, Command};

const STDERR_CAPTURE_LIMIT_BYTES: usize = 256 * 1024;
const STDERR_READ_BUFFER_BYTES: usize = 8 * 1024;

pub struct DumpChild {
    child: Child,
    pub stdout: ChildStdout,
    stderr_task: tokio::task::JoinHandle<Result<String>>,
}

pub struct DumpExit {
    pub success: bool,
    pub stderr: String,
}

pub fn spawn_mongodump(uri: &str, use_oplog: bool) -> Result<DumpChild> {
    let mut args = vec![
        "--uri".to_owned(),
        uri.to_owned(),
        "--archive".to_owned(),
        "--gzip".to_owned(),
    ];
    if use_oplog {
        args.push("--oplog".to_owned());
    }

    let mut cmd = Command::new("mongodump");
    cmd.args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);

    let mut child = cmd.spawn().context("failed to start mongodump process")?;
    let stdout = child
        .stdout
        .take()
        .context("mongodump stdout stream was not available")?;
    let stderr = child
        .stderr
        .take()
        .context("mongodump stderr stream was not available")?;

    let stderr_task = tokio::spawn(async move { read_stderr_tail(stderr).await });

    Ok(DumpChild {
        child,
        stdout,
        stderr_task,
    })
}

async fn read_stderr_tail(mut stderr: ChildStderr) -> Result<String> {
    let mut tail = Vec::new();
    let mut read_buf = [0_u8; STDERR_READ_BUFFER_BYTES];
    let mut truncated = false;

    loop {
        let read = stderr
            .read(&mut read_buf)
            .await
            .context("failed reading mongodump stderr stream")?;
        if read == 0 {
            break;
        }

        trim_to_capacity(&mut tail, read, STDERR_CAPTURE_LIMIT_BYTES, &mut truncated);
        tail.extend_from_slice(&read_buf[..read]);
    }

    let stderr = String::from_utf8_lossy(&tail).trim().to_owned();
    if !truncated {
        return Ok(stderr);
    }

    if stderr.is_empty() {
        return Ok(format!(
            "(mongodump stderr truncated to last {STDERR_CAPTURE_LIMIT_BYTES} bytes)"
        ));
    }

    Ok(format!(
        "(mongodump stderr truncated to last {STDERR_CAPTURE_LIMIT_BYTES} bytes) {stderr}"
    ))
}

fn trim_to_capacity(
    buffer: &mut Vec<u8>,
    incoming_len: usize,
    max_len: usize,
    truncated: &mut bool,
) {
    if buffer.len() + incoming_len <= max_len {
        return;
    }

    *truncated = true;
    let overflow = buffer.len() + incoming_len - max_len;
    if overflow >= buffer.len() {
        buffer.clear();
        return;
    }
    buffer.drain(..overflow);
}

impl DumpChild {
    pub async fn kill(&mut self) -> Result<()> {
        self.child
            .kill()
            .await
            .context("failed to kill mongodump process")
    }

    pub async fn wait(mut self) -> Result<DumpExit> {
        let status = self
            .child
            .wait()
            .await
            .context("failed waiting for mongodump process")?;
        let stderr = self.stderr_task.await.context("stderr task join error")??;
        Ok(DumpExit {
            success: status.success(),
            stderr,
        })
    }
}

pub async fn verify_binary(name: &str) -> Result<()> {
    let output = Command::new(name)
        .arg("--version")
        .kill_on_drop(true)
        .output()
        .await
        .with_context(|| format!("failed to execute `{name} --version`"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("binary `{name}` exists but returned non-zero status: {stderr}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::trim_to_capacity;

    #[test]
    fn trim_to_capacity_keeps_tail() {
        let mut buffer = b"123456".to_vec();
        let mut truncated = false;

        trim_to_capacity(&mut buffer, 4, 8, &mut truncated);

        assert_eq!(buffer, b"3456");
        assert!(truncated);
    }

    #[test]
    fn trim_to_capacity_noop_when_under_limit() {
        let mut buffer = b"1234".to_vec();
        let mut truncated = false;

        trim_to_capacity(&mut buffer, 2, 8, &mut truncated);

        assert_eq!(buffer, b"1234");
        assert!(!truncated);
    }
}
