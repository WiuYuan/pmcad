#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "gene_match.h"
#include "reader.h"
#include "uniprot_importer.h"

namespace py = pybind11;

PYBIND11_MODULE(_core, m) {
    m.doc() = "pmcad core C++ extension module";

    // ================= Reader =================
    m.def("read_tsv_file", &pmcad::Reader::read_tsv_file,
          "Read a single TSV file", py::arg("filename"));

    m.def("read_multi_tsv", &pmcad::Reader::read_multi_tsv,
          "Read multiple TSV files", py::arg("filelist"));

    m.def("read_tsv_safe", &pmcad::Reader::read_tsv_safe,
          "Read TSV file with error handling",
          py::arg("filename"), py::arg("skip_errors") = true);

    m.def("read_tsv_as_double", &pmcad::Reader::read_tsv_as_double,
          "Read TSV file and convert to double",
          py::arg("filename"));

    m.def("read_tsv_as_int", &pmcad::Reader::read_tsv_as_int,
          "Read TSV file and convert to int",
          py::arg("filename"));

    m.def("find_files", &pmcad::Reader::find_files,
          "Find files with given pattern in a directory",
          py::arg("foldername"), py::arg("pattern"));

    m.def("insert_files_to_pgdb", &pmcad::Reader::insert_files_to_pgdb,
          py::arg("filelist"), py::arg("table_name"),
          py::arg("dbname"), py::arg("user"),
          py::arg("password"), py::arg("host") = "localhost",
          py::arg("port") = "5432", py::arg("verbose") = false,
          "Insert TSV files into PostgreSQL database");

    // ================= GeneMatch =================
    m.def("match_reference", &pmcad::GeneMatch::match_reference,
          "Match the gene query to reference data",
          py::arg("query"), py::arg("reference"),
          py::arg("verbose"));

    // ================= UniprotImporter =================
    py::class_<pmcad::UniprotImporter>(m, "UniprotImporter")
        // -------- FT parser binding --------
        .def_static(
            "ft_stream_parse_and_copy",
            &pmcad::UniprotImporter::ft_stream_parse_and_copy,
            py::arg("gz_path"),
            py::arg("table_name"),
            py::arg("dbname"),
            py::arg("user"),
            py::arg("password"),
            py::arg("host") = "localhost",
            py::arg("port") = "5432",
            py::arg("batch_commit") = 200000,
            py::arg("verbose") = true,
            R"doc(
Stream-parse UniProt .dat.gz Feature Table (FT) records and import directly into PostgreSQL.

Each FT record is written as soon as it is parsed, using COPY FROM STDIN for high throughput.

Parameters
----------
gz_path : str
    Path to the .dat.gz UniProt file.
table_name : str
    Target PostgreSQL table name.
dbname : str
    Database name.
user : str
    PostgreSQL username.
password : str
    Password for the database.
host : str, optional
    Host address, default "localhost".
port : str, optional
    Port number, default "5432".
batch_commit : int, optional
    Commit every N records (default 200,000).
verbose : bool, optional
    Whether to print real-time progress (default True).

Table schema created automatically:
    id SERIAL PRIMARY KEY,
    accession TEXT,
    feature_type TEXT,
    start_pos INT,
    end_pos INT,
    note TEXT,
    evidence TEXT
)doc")
        // -------- DR parser binding --------
        .def_static(
            "dr_stream_parse_and_copy",
            &pmcad::UniprotImporter::dr_stream_parse_and_copy,
            py::arg("gz_path"),
            py::arg("table_name"),
            py::arg("dbname"),
            py::arg("user"),
            py::arg("password"),
            py::arg("host") = "localhost",
            py::arg("port") = "5432",
            py::arg("batch_commit") = 200000,
            py::arg("verbose") = true,
            R"doc(
Stream-parse UniProt .dat.gz Database Reference (DR) records and import directly into PostgreSQL.

Each DR record is parsed as:
    DR   <DB>; <ID>; <Description>; <Evidence>.

Supports major cross-references including:
    - GO; InterPro; Pfam; PROSITE; SMART; PANTHER; SUPFAM etc.

Parameters
----------
gz_path : str
    Path to the .dat.gz UniProt file.
table_name : str
    Target PostgreSQL table name (suggest using *_dr suffix).
dbname : str
    Database name.
user : str
    PostgreSQL username.
password : str
    Password for the database.
host : str, optional
    Host address, default "localhost".
port : str, optional
    Port number, default "5432".
batch_commit : int, optional
    Commit every N records (default 200,000).
verbose : bool, optional
    Whether to print real-time progress (default True).

Table schema created automatically:
    id SERIAL PRIMARY KEY,
    accession TEXT,
    db_name TEXT,
    db_id TEXT,
    description TEXT,
    evidence TEXT
)doc")

        // -------- SQ parser binding --------
        .def_static(
            "sq_stream_parse_and_copy",
            &pmcad::UniprotImporter::sq_stream_parse_and_copy,
            py::arg("gz_path"),
            py::arg("table_name"),
            py::arg("dbname"),
            py::arg("user"),
            py::arg("password"),
            py::arg("host") = "localhost",
            py::arg("port") = "5432",
            py::arg("batch_commit") = 20000,
            py::arg("verbose") = true,
            R"doc(
Stream-parse UniProt .dat.gz Sequence (SQ) records and import directly into PostgreSQL.

Each entry is parsed as:
    SQ   SEQUENCE   <Length> AA;  <MW> MW;  <CRC64> CRC64;
         <SEQUENCE LINES>
    //

The following fields are extracted:
    accession, length, mol_weight, crc64, sequence

Parameters
----------
gz_path : str
    Path to the .dat.gz UniProt file.
table_name : str
    Target PostgreSQL table name (suggest using *_sq suffix).
dbname : str
    Database name.
user : str
    PostgreSQL username.
password : str
    Password for the database.
host : str, optional
    Host address, default "localhost".
port : str, optional
    Port number, default "5432".
batch_commit : int, optional
    Commit every N records (default 20,000).
verbose : bool, optional
    Whether to print real-time progress (default True).

Table schema created automatically:
    id SERIAL PRIMARY KEY,
    accession TEXT,
    length INT,
    mol_weight INT,
    crc64 TEXT,
    sequence TEXT

Example output:
    ✅ Completed SQ import into table: uniprot_sprot_sq (550000 sequences)
)doc");
}