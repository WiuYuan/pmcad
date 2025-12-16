// src/cpp/uniprot_importer.h
#ifndef PMC_UNIPROT_IMPORTER_H
#define PMC_UNIPROT_IMPORTER_H

#include <string>

namespace pmcad {

/**
 * @class UniprotImporter
 * @brief 边解压边解析 UniProt .dat.gz，并以 COPY 模式写入 PostgreSQL。
 *
 * 通过 libpqxx::stream_to 实现高速流式导入，内存占用恒定。
 *
 * 可选参数 verbose 用于打印进度条（百分比、速率、ETA）。
 */
class UniprotImporter {
public:
    /**
     * @brief 从 UniProt .dat.gz 文件流式解析 Feature (FT) 段落并导入 PostgreSQL
     *
     * 每解析到一条 Feature 即写入数据库；
     * 按 batch_commit 条数批量提交事务，适合超大 TrEMBL 文件。
     *
     * @param gz_path 输入 gzip 文件路径 (.dat.gz)
     * @param table_name 目标数据库表名
     * @param dbname 数据库名
     * @param user 数据库用户名
     * @param password 数据库密码
     * @param host 数据库主机地址（默认 "localhost"）
     * @param port 数据库端口号（默认 "5432"）
     * @param batch_commit 每次提交事务的记录数（默认 200,000）
     * @param verbose 是否打印实时进度（默认 true）
     *
     * 表结构自动创建：
     *   id SERIAL PRIMARY KEY,
     *   accession TEXT,
     *   feature_type TEXT,
     *   start_pos INT,
     *   end_pos INT,
     *   note TEXT,
     *   evidence TEXT
     */
    static void ft_stream_parse_and_copy(
        const std::string& gz_path,
        const std::string& table_name,
        const std::string& dbname,
        const std::string& user,
        const std::string& password,
        const std::string& host = "localhost",
        const std::string& port = "5432",
        std::size_t batch_commit = 200000,
        bool verbose = true);

    /**
     * @brief 从 UniProt .dat.gz 文件流式解析 DR (Database cross-reference) 段落并导入 PostgreSQL
     *
     * 支持解析以下常见数据库引用：
     *   - GO; GO:xxxxxxx; F/P/C:term; IEA:Source.
     *   - InterPro; IPRxxxxx; Description.
     *   - Pfam; PFxxxxx; Description; Count.
     *   - PROSITE; PSxxxxx; Description; Count.
     *   - SMART / SUPFAM / PANTHER 等。
     *
     * 每行匹配 "DR   ..." 格式后提取以下字段写入数据库：
     *   accession, db_name, db_id, description, evidence
     *
     * 表结构自动创建：
     *   id SERIAL PRIMARY KEY,
     *   accession TEXT,
     *   db_name TEXT,
     *   db_id TEXT,
     *   description TEXT,
     *   evidence TEXT
     *
     * 输出示例：
     *   [12.3%] 105MB / 950MB, Imported: 200000 | Speed: 6.1 MB/s
     *
     * @param gz_path 输入 gzip 文件路径 (.dat.gz)
     * @param table_name 目标数据库表名（建议 *_dr）
     * @param dbname 数据库名
     * @param user 数据库用户名
     * @param password 数据库密码
     * @param host 数据库主机地址（默认 "localhost"）
     * @param port 数据库端口号（默认 "5432"）
     * @param batch_commit 每次提交事务的记录数（默认 200,000）
     * @param verbose 是否打印实时进度（默认 true）
     */
    static void dr_stream_parse_and_copy(
        const std::string& gz_path,
        const std::string& table_name,
        const std::string& dbname,
        const std::string& user,
        const std::string& password,
        const std::string& host = "localhost",
        const std::string& port = "5432",
        std::size_t batch_commit = 200000,
        bool verbose = true);
    
    /**
     * @brief 从 UniProt .dat.gz 文件流式解析 SQ (Sequence) 段落并导入 PostgreSQL
     *
     * 解析示例：
     *   SQ   SEQUENCE   438 AA;  48297 MW;  075C8FA17B3C5C56 CRC64;
     *        MARPLLGKTS SVRRRLESLS ACSIFFFLRK FCQKMASLVF LNSPVYQMSN ...
     *        ...
     *   //
     *
     * 每条记录提取以下字段：
     *   accession, length, mol_weight, crc64, sequence
     *
     * 表结构自动创建：
     *   id SERIAL PRIMARY KEY,
     *   accession TEXT,
     *   length INT,
     *   mol_weight INT,
     *   crc64 TEXT,
     *   sequence TEXT
     *
     * 每读取一条完整序列后立即写入数据库；
     * 按 batch_commit 条数批量提交事务，适合大规模 UniProt 数据导入。
     *
     * 输出示例：
     *   ✅ Completed SQ import into table: uniprot_sprot_sq (550000 sequences)
     *
     * @param gz_path 输入 gzip 文件路径 (.dat.gz)
     * @param table_name 目标数据库表名（建议 *_sq）
     * @param dbname 数据库名
     * @param user 数据库用户名
     * @param password 数据库密码
     * @param host 数据库主机地址（默认 "localhost"）
     * @param port 数据库端口号（默认 "5432"）
     * @param batch_commit 每次提交事务的记录数（默认 20,000）
     * @param verbose 是否打印实时进度（默认 true）
     */
     static void sq_stream_parse_and_copy(
        const std::string& gz_path,
        const std::string& table_name,
        const std::string& dbname,
        const std::string& user,
        const std::string& password,
        const std::string& host = "localhost",
        const std::string& port = "5432",
        std::size_t batch_commit = 20000,
        bool verbose = true);
};

} // namespace pmcad

#endif // PMC_UNIPROT_IMPORTER_H