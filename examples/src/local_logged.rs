use async_trait::async_trait;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use slatedb::Db;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Wraps an object store and appends one line per request to a log file.
struct LoggingStore {
    inner: Arc<dyn ObjectStore>,
    log: Arc<Mutex<File>>,
}

impl LoggingStore {
    fn record(&self, msg: String) {
        record(&self.log, msg);
    }
}

fn record(log: &Mutex<File>, msg: String) {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    let _ = writeln!(log.lock().unwrap(), "{ms} {msg}");
}

impl fmt::Debug for LoggingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LoggingStore({})", self.inner)
    }
}

impl fmt::Display for LoggingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "LoggingStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for LoggingStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let op = if options.head { "HEAD" } else { "GET" };
        self.record(format!("{op} {location} range={:?}", options.range));
        self.inner.get_opts(location, options).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.record(format!(
            "PUT {location} bytes={} mode={:?}",
            payload.content_length(),
            opts.mode
        ));
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.record(format!("PUT_MULTIPART {location}"));
        self.inner.put_multipart_opts(location, opts).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let log = self.log.clone();
        let locations = locations.inspect(move |p| {
            if let Ok(p) = p {
                record(&log, format!("DELETE {p}"));
            }
        });
        self.inner.delete_stream(locations.boxed())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.record(format!("LIST {prefix:?}"));
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.record(format!("LIST_WITH_OFFSET {prefix:?} offset={offset}"));
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.record(format!("LIST_WITH_DELIMITER {prefix:?}"));
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.record(format!("COPY {from} -> {to}"));
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.record(format!("RENAME {from} -> {to}"));
        self.inner.rename_opts(from, to, options).await
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let root = "/sam/warehouse/slatedb";
    std::fs::create_dir_all(root)?;
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open("object_store.log")?;
    let store = Arc::new(LoggingStore {
        inner: Arc::new(LocalFileSystem::new_with_prefix(root)?),
        log: Arc::new(Mutex::new(log)),
    });

    let db = Db::open("demo_db", store).await?;
    for i in 0..5 {
        db.put(format!("key{i}"), format!("value{i}")).await?;
    }
    for i in 0..6 {
        match db.get(format!("key{i}")).await? {
            Some(v) => println!("key{i} = {}", String::from_utf8_lossy(&v)),
            None => println!("key{i} not found"),
        }
    }
    db.close().await?;
    Ok(())
}
