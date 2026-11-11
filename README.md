# 交易明细质量管道环境

- batches/：财务交易明细批次文件（CSV，UTF-8，带表头），共 12 个；字段为 txn_id、account、amount、currency、txn_date、batch_id。
- rules.yaml：业务方给定的质量规则清单，检查口径随本文件内容而定。

运行约定：脚本从仓库根目录以 python3 pipeline.py 方式运行；批次数据与规则文件用相对路径 batches/ 与 rules.yaml。
