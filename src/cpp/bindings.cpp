#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "gene_match.h"
#include "reader.h"

namespace py = pybind11;

PYBIND11_MODULE(_core, m) {
    m.doc() = "pmcad core C++ extension module";

    // 直接绑定函数
    m.def("read_tsv_file", &pmcad::Reader::read_tsv_file,
          "Read a single TSV file", py::arg("filename"));

    m.def("read_multi_tsv", &pmcad::Reader::read_multi_tsv,
          "Read multiple TSV files", py::arg("filelist"));

    m.def("read_tsv_safe", &pmcad::Reader::read_tsv_safe,
          "Read TSV file with error handling",
          py::arg("filename"), py::arg("skip_errors") = true);

    m.def("read_tsv_as_double",
          &pmcad::Reader::read_tsv_as_double,
          "Read TSV file and convert to double",
          py::arg("filename"));

    m.def("read_tsv_as_int", &pmcad::Reader::read_tsv_as_int,
          "Read TSV file and convert to int",
          py::arg("filename"));

    m.def("find_files", &pmcad::Reader::find_files,
          "Find files with given pattern in a directory",
          py::arg("foldername"), py::arg("pattern"));

    m.def("match_reference", &pmcad::GeneMatch::match_reference,
          "Match the gene query to reference data, considering "
          "various patterns",
          py::arg("query"), py::arg("reference"),
          py::arg("verbose"));

    m.def("insert_files_to_pgdb", &pmcad::Reader::insert_files_to_pgdb,
          py::arg("filelist"), py::arg("table_name"),
          py::arg("dbname"), py::arg("user"),
          py::arg("password"), py::arg("host") = "localhost",
          py::arg("port") = "5432", py::arg("verbose") = false,
          "Insert TSV files into PostgreSQL database");
}