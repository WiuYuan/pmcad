#pragma once
#include <string>
#include <vector>

namespace pmcad {

class Reader {
public:
    static std::vector<std::vector<std::string> > read_tsv_file(
        const std::string& filename);
    static std::vector<std::vector<std::string> >
    read_multi_tsv(const std::vector<std::string>& filelist);

    // 新增功能：带错误处理的读取
    static std::vector<std::vector<std::string> > read_tsv_safe(
        const std::string& filename, bool skip_errors = true);

    // 新增功能：读取为特定数据类型
    static std::vector<std::vector<double> > read_tsv_as_double(
        const std::string& filename);
    static std::vector<std::vector<int> > read_tsv_as_int(
        const std::string& filename);

    // 新增功能：根据前缀查找文件夹下的所有子文件夹中文件名以给定前缀开头的文件
    static std::vector<std::string> find_files(
        const std::string& foldername,
        const std::string& pattern);

    static void insert_files_to_pgdb(
        const std::vector<std::string>& filelist,
        const std::string& table_name,
        const std::string& dbname, const std::string& user,
        const std::string& password,
        const std::string& host,
        const std::string& port, bool verbose);
};

} // namespace pmcad