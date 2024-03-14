#![allow(clippy::needless_option_as_deref)]
use pyo3::prelude::pyfunction;
use pyo3::PyResult;

#[pyfunction]
pub fn example_sql() -> PyResult<String> {
    Ok(queryer::example_sql())
}