#[cfg(any(target_os = "linux", target_os = "windows"))]
pub fn set_thread_affinity(core_ids: &[usize]) -> std::io::Result<()> {
    affinity::set_thread_affinity(core_ids)
        .map_err(|error| std::io::Error::other(error.to_string()))
}

#[cfg(any(target_os = "linux", target_os = "windows"))]
pub fn get_thread_affinity() -> std::io::Result<Vec<usize>> {
    affinity::get_thread_affinity().map_err(|error| std::io::Error::other(error.to_string()))
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
pub fn set_thread_affinity(_core_ids: &[usize]) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "thread affinity is not supported on this OS",
    ))
}

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
pub fn get_thread_affinity() -> std::io::Result<Vec<usize>> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "thread affinity is not supported on this OS",
    ))
}
