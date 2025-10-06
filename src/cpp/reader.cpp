#include "reader.h"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <pqxx/pqxx>
#include <regex>
#include <sstream>
#include <stdexcept>

namespace fs = std::filesystem;

namespace pmcad {

std::vector<std::string> Reader::find_files(
    const std::string& foldername, const std::string& pattern) {
    std::vector<std::string> result;

    // 创建正则表达式
    std::regex regex_pattern(pattern);

    // 使用递归遍历目录
    for (const auto& entry :
         fs::recursive_directory_iterator(foldername)) {
        if (entry.is_regular_file()) {
            std::string filename =
                entry.path().filename().string();

            // 检查文件名是否匹配正则表达式
            if (std::regex_match(filename, regex_pattern)) {
                // 如果文件名匹配正则表达式，记录路径
                result.push_back(entry.path().string());
            }
        }
    }

    return result;
}

std::vector<std::vector<std::string> > Reader::read_tsv_file(
    const std::string& filename) {
    std::vector<std::vector<std::string> > data;
    std::ifstream file(filename);

    if (!file.is_open()) {
        throw std::runtime_error("Cannot open file: " +
                                 filename);
    }

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty()) continue; // 跳过空行

        std::stringstream ss(line);
        std::string value;
        std::vector<std::string> row;

        while (std::getline(ss, value, '\t')) {
            row.push_back(value);
        }
        data.push_back(row);
    }

    return data;
}

std::vector<std::vector<std::string> > Reader::read_multi_tsv(
    const std::vector<std::string>& filelist) {
    std::vector<std::vector<std::string> > all_data;
    bool first_file = true; // 标记是否是第一个文件

    for (const auto& file : filelist) {
        try {
            auto data = read_tsv_file(file);

            if (first_file) {
                // 如果是第一个文件，保存表头并合并所有数据
                all_data = data;
                first_file =
                    false; // 设置为 false，以后不再重复添加表头
            } else {
                // 如果不是第一个文件，从第二行开始添加数据（跳过表头）
                all_data.insert(all_data.end(),
                                data.begin() + 1, data.end());
            }
        } catch (const std::exception& e) {
            std::cerr << "Warning: Failed to read " << file
                      << ": " << e.what() << std::endl;
        }
    }

    return all_data; // 返回合并后的数据
}

std::vector<std::vector<std::string> > Reader::read_tsv_safe(
    const std::string& filename, bool skip_errors) {
    std::vector<std::vector<std::string> > data;
    std::ifstream file(filename);

    if (!file.is_open()) {
        if (skip_errors) {
            return data;
        }
        throw std::runtime_error("Cannot open file: " +
                                 filename);
    }

    std::string line;
    size_t line_num = 0;

    while (std::getline(file, line)) {
        line_num++;

        if (line.empty()) continue;

        try {
            std::stringstream ss(line);
            std::string value;
            std::vector<std::string> row;

            while (std::getline(ss, value, '\t')) {
                row.push_back(value);
            }
            data.push_back(row);
        } catch (const std::exception& e) {
            if (!skip_errors) {
                throw std::runtime_error(
                    "Error reading line " +
                    std::to_string(line_num) + " in " +
                    filename);
            }
        }
    }

    return data;
}

std::vector<std::vector<double> > Reader::read_tsv_as_double(
    const std::string& filename) {
    auto string_data = read_tsv_file(filename);
    std::vector<std::vector<double> > result;

    for (const auto& row : string_data) {
        std::vector<double> double_row;
        for (const auto& cell : row) {
            try {
                double_row.push_back(std::stod(cell));
            } catch (const std::exception&) {
                double_row.push_back(
                    0.0); // 转换失败时使用默认值
            }
        }
        result.push_back(double_row);
    }

    return result;
}

std::vector<std::vector<int> > Reader::read_tsv_as_int(
    const std::string& filename) {
    auto string_data = read_tsv_file(filename);
    std::vector<std::vector<int> > result;

    for (const auto& row : string_data) {
        std::vector<int> int_row;
        for (const auto& cell : row) {
            try {
                int_row.push_back(std::stoi(cell));
            } catch (const std::exception&) {
                int_row.push_back(0); // 转换失败时使用默认值
            }
        }
        result.push_back(int_row);
    }

    return result;
}

void Reader::insert_files_to_pgdb(
    const std::vector<std::string>& filelist,
    const std::string& table_name, const std::string& dbname,
    const std::string& user, const std::string& password,
    const std::string& host = "localhost",
    const std::string& port = "5432", bool verbose = false) 
{
    try {
        // 构建连接字符串
        std::string conn_str =
            "dbname=" + dbname + " user=" + user +
            " password=" + password + " host=" + host +
            " port=" + port;

        pqxx::connection conn(conn_str);
        if (!conn.is_open()) {
            throw std::runtime_error("Failed to connect to the database!");
        }

        pqxx::work txn(conn);

        size_t total_files = filelist.size();
        bool first_file = true;

        for (size_t current = 0; current < total_files; ++current) {
            const auto& file = filelist[current];

            // 显示进度条
            if (verbose) {
                float progress = static_cast<float>(current + 1) / total_files;
                int bar_width = 50;
                int pos = static_cast<int>(bar_width * progress);

                std::cout << "\rProcessing files: [";
                for (int i = 0; i < bar_width; ++i) {
                    if (i < pos)
                        std::cout << "=";
                    else if (i == pos)
                        std::cout << ">";
                    else
                        std::cout << " ";
                }
                std::cout << "] " << std::setw(3)
                          << int(progress * 100) << "% ("
                          << current + 1 << "/" << total_files
                          << ")";
                std::cout << std::flush;
            }

            auto all_data = read_tsv_file(file);
            if (all_data.empty()) continue;

            if (first_file) {
                // 建表：只用第一个文件的表头
                const auto& header = all_data[0];
                std::string create_sql = "CREATE TABLE IF NOT EXISTS " + table_name + " (";
                for (size_t i = 0; i < header.size(); i++) {
                    create_sql += "\"" + header[i] + "\" TEXT";
                    if (i != header.size() - 1) create_sql += ", ";
                }
                create_sql += ");";
                txn.exec(create_sql);
                first_file = false;
            }

            // 插入数据（跳过表头）
            for (size_t r = 1; r < all_data.size(); r++) {
                const auto& row = all_data[r];

                // 使用 txn.esc 拼接 SQL，避免 exec_params 不支持迭代器的问题
                std::string insert_sql = "INSERT INTO " + table_name + " VALUES (";
                for (size_t i = 0; i < row.size(); ++i) {
                    insert_sql += "'" + txn.esc(row[i]) + "'";
                    if (i != row.size() - 1) insert_sql += ",";
                }
                insert_sql += ")";
                txn.exec(insert_sql);
            }
        }

        txn.commit();

        if (verbose) {
            std::cout << "\nAll files imported successfully into table: " << table_name << std::endl;
        }

    } catch (const std::exception& e) {
        std::cerr << "Error importing data into database: " << e.what() << std::endl;
    }
}

} // namespace pmcad