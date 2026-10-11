# 2026-10-11 自动化删减上线回执

实现见[PR #296](https://github.com/YuanYeYouTao/Yuki/pull/296)及[任务书](../architecture/Yuki-自动化删减与指令完整性任务书-2026-10-11.md)。D01–D63已实施，V01–V32按各行约定的离线或线上范围完成；没有新增完成裁判、强制发送、尾标检查器或失败重放。

## 提交、验证与镜像

| 项目 | 实际结果 |
| --- | --- |
| 实现提交 / 合并提交 | `c748ee10d75ca0580f4cd54c17958b20d75dda28` / `f439a267561f535deb0e218bb109359538edca15` |
| [实现PR Quality](https://github.com/YuanYeYouTao/Yuki/actions/runs/38136917861) | completed / success |
| [合并提交main Quality](https://github.com/YuanYeYouTao/Yuki/actions/runs/38136955488) | completed / success；1481 passed、63 skipped、1315 warnings、922.16s；前端18 passed |
| 本地验证 | Linux全源码Mypy643文件、Ruff/format/diff、前端测试及构建通过 |
| 本地pinned Worker完整pytest | 实际1538 passed、6 failed、0 skipped；六个旧夹具/退役调用修订后仅补跑原失败节点，全部通过。跨运行覆盖1544唯一节点，不称单次1544绿 |
| 镜像 | `ghcr.io/yuanyeyoutao/yuki-qqbot:ops-f439a267`，本地从合并提交构建direct |
| 实际镜像ID | `sha256:706448096f8ec21c2ba700b7071e633cadd6a1229c1ee3b9cbca7ae53412a4b9` |
| 上传归档SHA256 | `fe44d383e737f4b227d80ff33510ed8ca2370afb90d532ae1dfe60f2e27224ef`，服务器复核一致 |
| 本地部署smoke | source-free启动、head0106、无Monty binding/worker/launcher、重建与数据持久化通过；首次夹具缺persona，补齐原配置文件后通过，未改源码 |

正式3.9.0仍未发行；没有创建tag、GitHub Release或覆盖正式镜像。

## 生产更新

沿实际运行容器标签取得全部Compose覆盖、项目名和挂载；只更新Bot。停写后使用SQLite backup API取得一致备份，原库完整性和外键检查通过；在停写副本演练0105→0106后核完整性、外键和六类原账本数量，再升级生产库并复核。没有stamp或旧库覆盖。

| 副本迁移前后相同的账本 | 行数 |
| --- | ---: |
| automations / automation_versions / automation_runs | 118 / 76 / 151 |
| runtime_work | 147 |
| social_operation_receipts / chat_events | 16652 / 90244 |

旧Version的原委托与旧Run引用保持NULL，不从当前权限或同hash补造历史。完整性检查随机读取较多磁盘页，本次停写比预估长；后续可先准备并核验一致副本，将停写窗口留给最后一致备份与实际迁移，不新增产品机制。

13个本轮退役环境键已从`.env`清退。最终容器复核又发现旧Compose的内联`environment: !override`快照仍带同名键，精准删除这13项；对比完整effective Compose，只有Bot环境少这些键，其余配置、模型和服务保持相同。仅重建Bot后，实际环境没有这些键。

2026-10-11 12:20:52 UTC最终核验：

- Bot容器`be29aa1542ca92ecc127b655717eb7dddee89fa6bed0ff5b592128fc2642e894`，实际源码revision为`f439a267`，schema head为0106；Docker healthy，health.status/database均ok，OneBot连接，Automation Worker运行。
- Work active/wait均0，scheduler和wait最近错误均空。128/129各只有原一次成功Run；清环境后重建没有重发或复活。
- SnowLuma容器仍为`cbdabcdf7c2fff6763d3688b10f606b26c6be06ffce98cfba8bccd2032850d89`，StartedAt仍为`2026-10-09T05:10:47.99332112Z`。未重启或更新SnowLuma。
- 一致恢复点位于服务器`/opt/yuki-qqbot/ops/automation-integrity-f439a267-20261011T115545Z/backup`。0106保留执行事实，旧0105镜像不兼容；成功启用后的恢复采用支持现行schema的代码或向前修复，不回灌旧库丢失新消息、文件和效果。

## 两次新群验收

通过既有授权Control管理员入口、实际owner与数字生命研究所场景创建新的128/129；不是伪造QQ入站，也没有重跑126/127。模型工具创建链由离线V01验证，线上管理员接纳、原版本、真实Gemini输入及显式Social发送分别取证。

| 事实 | 短样本 | 长样本 |
| --- | --- | --- |
| Automation / Run / 原Version | 128 / 182 / 77 | 129 / 183 / 78 |
| 展开A数 / instruction字符 | 32 / 318 | 5600 / 5885 |
| Work | `baa91a8a-5b4f-47da-b06e-8c5a100eb1ab` | `0e48547d-8812-4218-b01c-dee60256c987` |
| 入库与原命令完整JSON语义相同 | 是 | 是 |
| 主模型 / Gemini输入trace | 176604 / 176606 | 176619 / 176621、176631 / 176633 |
| 每次模型输入完整JSON语义相同 | 是 | 是 |
| 显式SEND trace / 原内部Event | 176615 / 95889 | 176630 / 95890 |
| 原Social operation | `3d0a0b5d-1a35-4e03-8a85-a1b9a87efd2e` | `af6fd8be-4cc8-4e2e-b7ae-5ef52929214c` |
| 实际发送正文 | `YUKI_AUTO_SHORT_OK_a5c6c87eec9d` | `YUKI_AUTO_LONG_OK_e28b9886e05e` |
| Social / Work / Run | succeeded / completed / succeeded | succeeded / completed / succeeded |
| models / tools / sends | 1 / 1 / 1 | 2 / 1 / 1 |
| max_runs / run_count / next_run_at / Run数 | 1 / 1 / NULL / 1 | 1 / 1 / NULL / 1 |

两条发送均由原回执内部Event ID及operation ID回查实际群账本。共享Social的origin为social_tool，旧automation冗余外键为空；不靠平台消息号或普通业务payload重建所有权。原Work/source/version/Run计量和终态保持，没有多建、充值或重复发送。
