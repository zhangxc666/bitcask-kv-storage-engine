# 基于bitcask模型的存储引擎

本项目是基于Bitcask存储模型的高性能kv存储引擎。提供简洁的用户API，可通过内嵌的接口或者redis-client进行直接连接访问。

## 设计细节

- **存储模型**：完成了基于**Golang**的**Bitcask**存储模型实现，支持高效的数据写入、删除操作。
- **索引**：使用了B树和B+树索引，高效、快速数据访问。
- **数据合并**： 实现了数据合并机制，有效清理无效数据，保持系统性能
- **批量写**：使用writebatch机制，实现了原子性的批量写入功能，确保了数据操作的一致性。
- **Redis协议支持**：扩展存储引擎以兼容Redis协议，实现了对Set, List, Hash, String, Sorted set等数据结构的部分命令支持

## 与Redis性能比较 (测试脚本在benchmark文件夹中)

- set
    - **bitcask**:
        ```azure
        Total time for 100000 requests: 1.2922730445861816 seconds
        Overall querys per second (QPS): 77383.02707693056
        ```
    - **redis**:
       ```azure
       Total time for 100000 requests: 1.0942137241363525 seconds
       Overall querys per second (QPS): 91389.82430414003
       ```

## 使用教程

1. 安装go（不低于1.20）
2. 在当前文件夹根目录下输入 `go mod tidy`
3. 进入`redis/cmd`命令，运行`server.go`
4. 安装redis-cli
5. 在终端输入`redis-cli -p 6380`
6. 进入可以进行命令输入，目前仅支持set，get，sadd，hset，lpush，zadd 命令

## TODO

- [ ] 索引锁粒度优化
- [ ] 数据文件布局优化
- [ ] 支持其他redis命令
- [ ] 完善与redis其他命令比较测试


