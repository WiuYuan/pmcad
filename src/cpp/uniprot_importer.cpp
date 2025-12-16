// src/cpp/uniprot_importer.cpp
#include <pqxx/pqxx>
#include <zlib.h>
#include <iostream>
#include <sstream>
#include <regex>
#include <vector>
#include <string>
#include <stdexcept>
#include <iomanip>
#include <chrono>
#include <sys/stat.h>

namespace pmcad {

static void ensure_table_ft(pqxx::connection& conn, const std::string& table) {
    pqxx::work t(conn);
    t.exec(
        "CREATE TABLE IF NOT EXISTS " + t.esc(table) + " ("
        "  id SERIAL PRIMARY KEY,"
        "  accession TEXT,"
        "  feature_type TEXT,"
        "  start_pos INT,"
        "  end_pos INT,"
        "  note TEXT,"
        "  evidence TEXT"
        ");"
    );
    t.commit();
}

static void ensure_table_dr(pqxx::connection& conn, const std::string& table) {
    pqxx::work t(conn);
    t.exec(
        "CREATE TABLE IF NOT EXISTS " + t.esc(table) + " ("
        "  id SERIAL PRIMARY KEY,"
        "  accession TEXT,"
        "  db_name TEXT,"
        "  db_id TEXT,"
        "  description TEXT,"
        "  evidence TEXT"
        ");"
    );
    t.commit();
}

/**
 * @brief 从 gzip 解压流式解析 UniProt db 并导入 PostgreSQL
 * @param verbose 是否打印进度条
 */
class UniprotImporter {
public:
    static void ft_stream_parse_and_copy(
        const std::string& gz_path,
        const std::string& table_name,
        const std::string& dbname,
        const std::string& user,
        const std::string& password,
        const std::string& host = "localhost",
        const std::string& port = "5432",
        std::size_t batch_commit = 200000,
        bool verbose = true
    );
    static void dr_stream_parse_and_copy(
        const std::string& gz_path,
        const std::string& table_name,
        const std::string& dbname,
        const std::string& user,
        const std::string& password,
        const std::string& host = "localhost",
        const std::string& port = "5432",
        std::size_t batch_commit = 200000,
        bool verbose = true
    );
    static void sq_stream_parse_and_copy(
        const std::string& gz_path,
        const std::string& table_name,
        const std::string& dbname,
        const std::string& user,
        const std::string& password,
        const std::string& host = "localhost",
        const std::string& port = "5432",
        std::size_t batch_commit = 200000,
        bool verbose = true
    );
};

void UniprotImporter::ft_stream_parse_and_copy(
    const std::string& gz_path,
    const std::string& table_name,
    const std::string& dbname,
    const std::string& user,
    const std::string& password,
    const std::string& host,
    const std::string& port,
    std::size_t batch_commit,
    bool verbose
) {
    // ---------- 数据库连接 ----------
    std::string conn_str =
        "dbname=" + dbname + " user=" + user + " password=" + password +
        " host=" + host + " port=" + port;
    pqxx::connection conn(conn_str);
    if (!conn.is_open())
        throw std::runtime_error("❌ Cannot connect to PostgreSQL");

    ensure_table_ft(conn, table_name);

    // ---------- 打开 gzip ----------
    gzFile gzfile = gzopen(gz_path.c_str(), "rb");
    if (!gzfile)
        throw std::runtime_error("❌ Cannot open gzip file: " + gz_path);

    // 获取文件大小用于进度估计
    struct stat st;
    size_t total_bytes = 0;
    if (stat(gz_path.c_str(), &st) == 0)
        total_bytes = st.st_size;

    const size_t BUF_SIZE = 16384;
    char buffer[BUF_SIZE];
    std::string line, accession, f_type, f_start, f_end, f_note, f_evi;
    bool in_feature = false;
    std::size_t written = 0;
    size_t processed_bytes = 0;

    std::regex ac_regex(R"(AC\s+([A-Z0-9]+);)");
    std::regex ft_regex(R"(^FT\s+(\S+)\s+(\d+)\.\.(\d+))");
    std::regex note_regex(R"REGEX(/note="([^"]+)")REGEX");
    std::regex evi_regex(R"REGEX(/evidence="([^"]+)")REGEX");

    std::unique_ptr<pqxx::work> tx;
    std::unique_ptr<pqxx::stream_to> writer;

    auto open_stream = [&](std::unique_ptr<pqxx::work>& tx,
                           std::unique_ptr<pqxx::stream_to>& writer) {
        tx = std::make_unique<pqxx::work>(conn);
        pqxx::table_path path{table_name};
        writer = std::make_unique<pqxx::stream_to>(
            pqxx::stream_to::table(
                *tx, path,
                {"accession", "feature_type", "start_pos", "end_pos", "note", "evidence"}
            )
        );
    };

    auto close_stream = [&](std::unique_ptr<pqxx::work>& tx,
                            std::unique_ptr<pqxx::stream_to>& writer) {
        if (writer) { writer->complete(); writer.reset(); }
        if (tx)     { tx->commit(); tx.reset(); }
    };

    open_stream(tx, writer);

    // ---------- 时间统计 ----------
    auto start_time = std::chrono::steady_clock::now();

    while (gzgets(gzfile, buffer, BUF_SIZE)) {
        line = buffer;
        line.erase(std::remove(line.begin(), line.end(), '\n'), line.end());
        if (!line.empty() && line.back() == '.')
            line.pop_back();
        processed_bytes += strlen(buffer);

        if (line.rfind("AC   ", 0) == 0) {
            std::smatch m;
            if (std::regex_search(line, m, ac_regex))
                accession = m[1];
            continue;
        }

        if (line.rfind("FT   ", 0) == 0) {
            std::smatch m;
            if (std::regex_search(line, m, ft_regex)) {
                if (in_feature) {
                    writer->write_values(accession, f_type, f_start, f_end, f_note, f_evi);
                    written++;
                    f_type.clear(); f_start.clear(); f_end.clear(); f_note.clear(); f_evi.clear();
                }
                f_type  = m[1];
                f_start = m[2];
                f_end   = m[3];
                in_feature = true;
            } else if (in_feature) {
                if (std::smatch mn; std::regex_search(line, mn, note_regex)) f_note = mn[1];
                if (std::smatch me; std::regex_search(line, me, evi_regex))  f_evi  = me[1];
            }
            continue;
        }

        if (line.rfind("//", 0) == 0) {
            if (in_feature) {
                writer->write_values(accession, f_type, f_start, f_end, f_note, f_evi);
                written++;
                in_feature = false;
                f_type.clear(); f_start.clear(); f_end.clear(); f_note.clear(); f_evi.clear();
            }

            // ---------- 进度显示 ----------
            if (verbose && written % 10000 == 0) {
                double ratio = total_bytes ? (double)processed_bytes / total_bytes : 0.0;
                auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                                   std::chrono::steady_clock::now() - start_time)
                                   .count();
                double rate = elapsed > 0 ? processed_bytes / (1024.0 * 1024.0 * elapsed) : 0.0;
                double eta = (ratio > 0 && rate > 0)
                                 ? (total_bytes - processed_bytes) / (rate * 1024 * 1024)
                                 : 0.0;

                std::cerr << "\r[" << std::setw(6) << std::fixed << std::setprecision(2)
                          << ratio * 100 << "%] "
                          << processed_bytes / (1024 * 1024) << "MB / "
                          << total_bytes / (1024 * 1024) << "MB, "
                          << "Imported: " << written
                          << " | Speed: " << std::fixed << std::setprecision(2)
                          << rate << " MB/s"
                          << " | ETA: " << std::fixed << std::setprecision(1)
                          << eta << "s   " << std::flush;
            }

            if (written >= batch_commit) {
                close_stream(tx, writer);
                open_stream(tx, writer);
                written = 0;
            }

            accession.clear();
            continue;
        }
    }

    if (in_feature) {
        writer->write_values(accession, f_type, f_start, f_end, f_note, f_evi);
        written++;
    }

    close_stream(tx, writer);
    gzclose(gzfile);

    auto end_time = std::chrono::steady_clock::now();
    auto total_s = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();

    std::cout << "\n✅ Completed import into table: " << table_name
              << " in " << total_s << "s.\n";
}

void UniprotImporter::dr_stream_parse_and_copy(
    const std::string& gz_path,
    const std::string& table_name,
    const std::string& dbname,
    const std::string& user,
    const std::string& password,
    const std::string& host,
    const std::string& port,
    std::size_t batch_commit,
    bool verbose
) {
    // ---------- 数据库连接 ----------
    std::string conn_str =
        "dbname=" + dbname + " user=" + user + " password=" + password +
        " host=" + host + " port=" + port;
    pqxx::connection conn(conn_str);
    if (!conn.is_open())
        throw std::runtime_error("❌ Cannot connect to PostgreSQL");

    ensure_table_dr(conn, table_name);

    // ---------- 打开 gzip ----------
    gzFile gzfile = gzopen(gz_path.c_str(), "rb");
    if (!gzfile)
        throw std::runtime_error("❌ Cannot open gzip file: " + gz_path);

    struct stat st;
    size_t total_bytes = 0;
    if (stat(gz_path.c_str(), &st) == 0)
        total_bytes = st.st_size;

    const size_t BUF_SIZE = 16384;
    char buffer[BUF_SIZE];
    std::string line, accession, db_name, db_id, desc, evidence;
    size_t processed_bytes = 0, written = 0;

    // 正则表达式
    std::regex ac_regex(R"(([A-Z0-9]+);)");
    std::regex dr_regex(R"(^DR\s+(\S+);\s*([^;]+)(?:;\s*([^;]+))?(?:;\s*(.*))?)");

    std::unique_ptr<pqxx::work> tx;
    std::unique_ptr<pqxx::stream_to> writer;

    auto open_stream = [&](std::unique_ptr<pqxx::work>& tx,
                           std::unique_ptr<pqxx::stream_to>& writer) {
        tx = std::make_unique<pqxx::work>(conn);
        pqxx::table_path path{table_name};
        writer = std::make_unique<pqxx::stream_to>(
            pqxx::stream_to::table(
                *tx, path,
                {"accession", "db_name", "db_id", "description", "evidence"}
            )
        );
    };

    auto close_stream = [&](std::unique_ptr<pqxx::work>& tx,
                            std::unique_ptr<pqxx::stream_to>& writer) {
        if (writer) { writer->complete(); writer.reset(); }
        if (tx)     { tx->commit(); tx.reset(); }
    };

    open_stream(tx, writer);

    auto start_time = std::chrono::steady_clock::now();
    std::vector<std::string> accessions;

    while (gzgets(gzfile, buffer, BUF_SIZE)) {
        line = buffer;
        line.erase(std::remove(line.begin(), line.end(), '\n'), line.end());
        if (!line.empty() && line.back() == '.')
            line.pop_back();
        processed_bytes += strlen(buffer);

        // 获取 accession !多AC问题需要处理
        if (line.rfind("AC   ", 0) == 0) {
            for (std::sregex_iterator it(line.begin(), line.end(), ac_regex), end; it != end; ++it) {
                accessions.push_back((*it)[1]);
            }
            continue;
        }

        // 解析 DR 行
        if (line.rfind("DR   ", 0) == 0) {
            std::smatch m;
            db_name.clear(); db_id.clear(); desc.clear(); evidence.clear();

            if (std::regex_search(line, m, dr_regex)) {
                db_name = m[1].matched ? m[1].str() : "";
                db_id   = m[2].matched ? m[2].str() : "";
                desc    = m[3].matched ? m[3].str() : "";
                evidence= m[4].matched ? m[4].str() : "";
            }

            if (!db_name.empty() && !accessions.empty()) {
                for (const auto &acc : accessions) {
                    writer->write_values(acc, db_name, db_id, desc, evidence);
                    written++;
                }
            }

            if (written % batch_commit == 0) {
                close_stream(tx, writer);
                open_stream(tx, writer);
            }
        }

        // 新条目时清空 accession
        if (line.rfind("//", 0) == 0) {
            accessions.clear();
            continue;
        }
        if (line.rfind("ID   ", 0) == 0) {
            accessions.clear();
            continue;
        }

        // 进度显示
        if (verbose && written % 10000 == 0) {
            double ratio = total_bytes ? (double)processed_bytes / total_bytes : 0.0;
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::steady_clock::now() - start_time)
                               .count();
            double rate = elapsed > 0 ? processed_bytes / (1024.0 * 1024.0 * elapsed) : 0.0;
            std::cerr << "\r[" << std::setw(6) << std::fixed << std::setprecision(2)
                      << ratio * 100 << "%] "
                      << processed_bytes / (1024 * 1024) << "MB / "
                      << total_bytes / (1024 * 1024) << "MB, Imported: "
                      << written << " | Speed: " << std::fixed << std::setprecision(2)
                      << rate << " MB/s" << std::flush;
        }
    }

    close_stream(tx, writer);
    gzclose(gzfile);

    auto end_time = std::chrono::steady_clock::now();
    auto total_s = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();

    std::cout << "\n✅ Completed DR import into table: " << table_name
              << " in " << total_s << "s.\n";
}

void UniprotImporter::sq_stream_parse_and_copy(
    const std::string& gz_path,
    const std::string& table_name,
    const std::string& dbname,
    const std::string& user,
    const std::string& password,
    const std::string& host,
    const std::string& port,
    std::size_t batch_commit,
    bool verbose
) {
    // ---------- 数据库连接 ----------
    std::string conn_str =
        "dbname=" + dbname + " user=" + user + " password=" + password +
        " host=" + host + " port=" + port;
    pqxx::connection conn(conn_str);
    if (!conn.is_open())
        throw std::runtime_error("❌ Cannot connect to PostgreSQL");

    // ---------- 建表 ----------
    {
        pqxx::work t(conn);
        t.exec(
            "CREATE TABLE IF NOT EXISTS " + t.esc(table_name) + " ("
            "  id SERIAL PRIMARY KEY,"
            "  accession TEXT,"
            "  length INT,"
            "  mol_weight INT,"
            "  crc64 TEXT,"
            "  sequence TEXT"
            ");"
        );
        t.commit();
    }

    // ---------- 打开 gzip ----------
    gzFile gzfile = gzopen(gz_path.c_str(), "rb");
    if (!gzfile)
        throw std::runtime_error("❌ Cannot open gzip file: " + gz_path);

    // 获取文件大小，用于进度估计
    struct stat st;
    size_t total_bytes = 0;
    if (stat(gz_path.c_str(), &st) == 0)
        total_bytes = st.st_size;

    const size_t BUF_SIZE = 16384;
    char buffer[BUF_SIZE];
    std::string line, accession, seq, crc64;
    int length = 0, mw = 0;
    bool in_seq = false;

    std::regex ac_regex(R"(AC\s+([A-Z0-9]+);)");
    std::regex sq_header_regex(R"(SQ\s+SEQUENCE\s+(\d+)\s+AA;\s+(\d+)\s+MW;\s+([A-F0-9]+)\s+CRC64;)");
    std::regex seq_line_regex(R"(^\s{5}([A-Z\s]+))");

    std::unique_ptr<pqxx::work> tx;
    std::unique_ptr<pqxx::stream_to> writer;

    auto open_stream = [&](std::unique_ptr<pqxx::work>& tx,
                           std::unique_ptr<pqxx::stream_to>& writer) {
        tx = std::make_unique<pqxx::work>(conn);
        pqxx::table_path path{table_name};
        writer = std::make_unique<pqxx::stream_to>(
            pqxx::stream_to::table(*tx, path,
                {"accession", "length", "mol_weight", "crc64", "sequence"})
        );
    };

    auto close_stream = [&](std::unique_ptr<pqxx::work>& tx,
                            std::unique_ptr<pqxx::stream_to>& writer) {
        if (writer) { writer->complete(); writer.reset(); }
        if (tx)     { tx->commit(); tx.reset(); }
    };

    open_stream(tx, writer);

    size_t written = 0;
    size_t processed_bytes = 0;
    auto start_time = std::chrono::steady_clock::now();

    // ---------- 主解析循环 ----------
    while (gzgets(gzfile, buffer, BUF_SIZE)) {
        line = buffer;
        line.erase(std::remove(line.begin(), line.end(), '\n'), line.end());
        processed_bytes += strlen(buffer);

        // 捕获 accession
        if (line.rfind("AC   ", 0) == 0) {
            std::smatch m;
            if (std::regex_search(line, m, ac_regex))
                accession = m[1];
            continue;
        }

        // SQ header
        if (line.rfind("SQ   ", 0) == 0) {
            std::smatch m;
            if (std::regex_search(line, m, sq_header_regex)) {
                length = std::stoi(m[1]);
                mw = std::stoi(m[2]);
                crc64 = m[3];
                seq.clear();
                in_seq = true;
            }
            continue;
        }

        // 读取序列行
        if (in_seq && line.rfind("     ", 0) == 0) {
            std::smatch m;
            if (std::regex_search(line, m, seq_line_regex)) {
                std::string part = m[1];
                part.erase(std::remove(part.begin(), part.end(), ' '), part.end());
                seq += part;
            }
            continue;
        }

        // 条目结束
        if (line.rfind("//", 0) == 0) {
            if (in_seq && !accession.empty()) {
                writer->write_values(accession, length, mw, crc64, seq);
                written++;
            }
            in_seq = false;
            accession.clear();
            seq.clear();
            crc64.clear();
            continue;
        }

        // 定期提交批次
        if (written % batch_commit == 0 && written > 0) {
            close_stream(tx, writer);
            open_stream(tx, writer);
        }

        // ---------- 实时进度 ----------
        if (verbose && written % 1000 == 0 && written > 0) {
            double ratio = total_bytes ? (double)processed_bytes / total_bytes : 0.0;
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::steady_clock::now() - start_time).count();
            double rate = elapsed > 0 ? processed_bytes / (1024.0 * 1024.0 * elapsed) : 0.0;
            std::cerr << "\r[" << std::setw(6) << std::fixed << std::setprecision(2)
                      << ratio * 100 << "%] "
                      << processed_bytes / (1024 * 1024) << "MB / "
                      << total_bytes / (1024 * 1024) << "MB, "
                      << "Imported: " << written
                      << " | Speed: " << std::fixed << std::setprecision(2)
                      << rate << " MB/s" << std::flush;
        }
    }

    close_stream(tx, writer);
    gzclose(gzfile);

    auto end_time = std::chrono::steady_clock::now();
    auto total_s = std::chrono::duration_cast<std::chrono::seconds>(end_time - start_time).count();

    std::cout << "\n✅ Completed SQ import into table: " << table_name
              << " (" << written << " sequences, "
              << total_s << "s elapsed)\n";
}

} // namespace pmcad