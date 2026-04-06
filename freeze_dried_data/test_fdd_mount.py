import os
import subprocess
import time
import shutil
import stat
import tempfile
from freeze_dried_data import WFDD
import random
from multiprocessing import Process


MOUNT_POINT = "/tmp/fdd_test_mount"
FDD_PATH = "/dev/shm/test_nested.fdd"
FUSE_SCRIPT = "./fdd_fuse.py"


def create_test_fdd(path):
    with WFDD(path, columns={"name": "str", "content": "bytes"}, overwrite=True) as wfdd:
        wfdd["flat"] = {"name": "root.txt", "content": b"top-level"}
        wfdd["nested"] = {"name": "a/b/c.txt", "content": b"hello"}
        wfdd["sibling"] = {"name": "a/b/d.txt", "content": b"world!"}
        wfdd["unicode_name"] = {"name": "unic🌍de/😊.txt", "content": b"utf name"}
        wfdd["unicode_content"] = {"name": "unicode_content.txt", "content": "你好，世界🌟".encode("utf-8")}
        wfdd["empty_file"] = {"name": "empty/file.txt", "content": b""}

        # Special chars
        wfdd["special_chars"] = {"name": "weird/this has #spaces$.txt", "content": b"strange chars"}
        wfdd["long_name"] = {
            "name": "very/" + ("a" * 100) + ".txt",
            "content": b"long filename"
        }

        # Null bytes
        wfdd["null_bytes"] = {"name": "bin/null.bin", "content": b"\x00\x01\x02\x03\x00"}

        # Multiple small files to push cache eviction
        for i in range(20):
            wfdd[f"tiny{i}"] = {"name": f"small/file_{i}.txt", "content": f"file{i}".encode("utf-8")}

        # Big file (~5MB)
        wfdd["big1"] = {"name": "big/big1.bin", "content": b"A" * (1024 * 1024 * 5)}
        wfdd["big2"] = {"name": "big/big2.bin", "content": b"B" * (1024 * 1024 * 6)}


def mount_fdd(path, mountpoint):
    os.makedirs(mountpoint, exist_ok=True)
    return subprocess.Popen([
        "python3", FUSE_SCRIPT,
        path,
        mountpoint,
        "--filename-column", "name",
        "--content-column", "content",
        "--filename-extension", "",
        "--cache-size", "1"  # 1 MB cache to force eviction
    ])


def wait_until_mounted(mountpoint, timeout=5):
    for _ in range(timeout * 10):
        try:
            if os.listdir(mountpoint):
                return
        except Exception:
            pass
        time.sleep(0.1)
    raise TimeoutError("Mount did not become available in time.")


def check_file(path, expected_content):
    assert os.path.isfile(path), f"Missing file: {path}"
    st = os.stat(path)
    assert stat.S_ISREG(st.st_mode), f"Not a file: {path}"
    with open(path, "rb") as f:
        data = f.read()
        assert data == expected_content, f"Content mismatch for {path}"


def run_tests(mountpoint):
    print("🔍 Basic structure")
    check_file(os.path.join(mountpoint, "root.txt"), b"top-level")
    check_file(os.path.join(mountpoint, "a/b/c.txt"), b"hello")
    check_file(os.path.join(mountpoint, "a/b/d.txt"), b"world!")
    check_file(os.path.join(mountpoint, "unic🌍de/😊.txt"), b"utf name")
    check_file(os.path.join(mountpoint, "unicode_content.txt"), "你好，世界🌟".encode("utf-8"))
    check_file(os.path.join(mountpoint, "empty/file.txt"), b"")
    check_file(os.path.join(mountpoint, "weird/this has #spaces$.txt"), b"strange chars")
    check_file(os.path.join(mountpoint, "very/" + ("a" * 100) + ".txt"), b"long filename")
    check_file(os.path.join(mountpoint, "bin/null.bin"), b"\x00\x01\x02\x03\x00")

    print("🔁 Reading many small files to stress cache")
    for i in range(20):
        check_file(os.path.join(mountpoint, f"small/file_{i}.txt"), f"file{i}".encode("utf-8"))

    print("📏 Check big file sizes")
    for name, size, char in [("big1.bin", 5 * 1024 * 1024, b"A"), ("big2.bin", 6 * 1024 * 1024, b"B")]:
        path = os.path.join(mountpoint, "big", name)
        assert os.path.isfile(path)
        st = os.stat(path)
        assert st.st_size == size, f"Expected size {size}, got {st.st_size} for {name}"

        with open(path, "rb") as f:
            assert f.read(10) == char * 10
            f.seek(size // 2)
            middle = f.read(10)
            assert middle == char * 10, f"Middle chunk wrong in {name}"
            f.seek(size - 10)
            tail = f.read()
            assert tail == char * 10, f"Tail wrong in {name}"

    print("📁 Validating directory contents")
    assert "c.txt" in os.listdir(os.path.join(mountpoint, "a/b"))
    assert "😊.txt" in os.listdir(os.path.join(mountpoint, "unic🌍de"))
    assert any("file_" in f for f in os.listdir(os.path.join(mountpoint, "small")))

    print("❌ Error paths")
    try:
        open(os.path.join(mountpoint, "doesnotexist.txt"), "rb")
        assert False, "Expected FileNotFoundError"
    except FileNotFoundError:
        pass

    try:
        os.listdir(os.path.join(mountpoint, "root.txt"))
        assert False, "Expected NotADirectoryError"
    except NotADirectoryError:
        pass

def concurrent_read_worker(mountpoint, filenames, rounds=10):
    for _ in range(rounds):
        fname = random.choice(filenames)
        fpath = os.path.join(mountpoint, fname)
        try:
            with open(fpath, "rb") as f:
                size = os.fstat(f.fileno()).st_size

                _ = f.read(128)
                if size > 10:
                    f.seek(10)
                    _ = f.read(64)
                    f.seek(-10, os.SEEK_END)
                    _ = f.read()
                else:
                    f.seek(0)
                    _ = f.read(size)
        except Exception as e:
            print(f"[Child] Failed to read {fpath}: {e}")


def unmount(mountpoint):
    subprocess.run(["fusermount", "-u", mountpoint], check=True)


def main():
    print("🛠 Creating FDD with complex data")
    create_test_fdd(FDD_PATH)

    print("🔗 Mounting FDD...")
    proc = mount_fdd(FDD_PATH, MOUNT_POINT)
    try:
        wait_until_mounted(MOUNT_POINT)
        print("✅ Running extended test suite...")
        run_tests(MOUNT_POINT)
        print("🧪 Starting concurrent stress test...")

        filenames = []
        for root, dirs, files in os.walk(MOUNT_POINT):
            for file in files:
                relpath = os.path.relpath(os.path.join(root, file), MOUNT_POINT)
                filenames.append(relpath)

        workers = [Process(target=concurrent_read_worker, args=(MOUNT_POINT, filenames, 50)) for _ in range(10)]
        for w in workers:
            w.start()
        for w in workers:
            w.join(timeout=10)
            assert not w.exitcode or w.exitcode == 0, f"Worker exited with code {w.exitcode}"

        print("✅ Concurrent access passed.")
        print("🎉 All tests passed.")
    finally:
        print("🧼 Cleaning up...")
        unmount(MOUNT_POINT)
        proc.terminate()
        proc.wait(timeout=5)
        shutil.rmtree(MOUNT_POINT, ignore_errors=True)
        if os.path.exists(FDD_PATH):
            os.remove(FDD_PATH)
        print("✅ Done.")


if __name__ == "__main__":
    main()
