# 织流工坊 · 网页流程自动化与RPA采集执行平台

面向RPA流程模板编排、元素定位漂移检测、机器人节点调度、执行步骤轨迹、凭据轮换、文件归集、采集批次确认与下游交接的流程自动化采集执行平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              25个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 流程模板 | `flow_template` | 流程模板 | 模板编号、流程名称、起始地址 |
| 元素定位 | `element_locator` | 定位策略 | 定位编号、所属模板、元素名称 |
| 机器人节点 | `robot_node` | 执行节点 | 节点编号、节点名称、运行环境 |
| 流程执行 | `flow_run` | 执行实例 | 执行编号、关联模板、执行节点 |
| 步骤轨迹 | `step_trace` | 步骤轨迹 | 轨迹编号、关联执行、步骤序号 |
| 任务队列 | `task_queue` | 排队任务 | 队列编号、关联模板、优先级 |
| 凭据保管 | `credential_vault` | 登录凭据 | 凭据编号、适用站点、账号标识 |
| 表单字段 | `form_field` | 填充字段 | 字段编号、关联模板、表单名称 |
| 文件归集 | `file_drop` | 归集文件 | 归集编号、关联执行、文件名称 |
| 识别区域 | `ocr_region` | 识别区域 | 区域编号、关联模板、区域名称 |
| 排程计划 | `schedule_plan` | 排程计划 | 计划编号、关联模板、触发周期 |
| 异常规则 | `exception_rule` | 处置规则 | 规则编号、匹配特征、判定条件 |
| 重试策略 | `retry_policy` | 重试策略 | 策略编号、关联规则、最大次数 |
| 采集批次 | `capture_batch` | 采集批次 | 批次编号、关联执行、记录行数 |
| 字段映射 | `field_mapping` | 映射规则 | 映射编号、来源字段、目标字段 |
| 数据落库 | `data_landing` | 落库记录 | 落库编号、关联批次、目标存储 |
| 审计轨迹 | `audit_trail` | 审计条目 | 条目编号、操作对象、操作类型 |
| 资源额度 | `resource_pool` | 资源额度 | 额度编号、资源类型、总额度 |
| 版本发布 | `version_release` | 发布版本 | 发布编号、关联模板、版本号 |
| 运行指标 | `metric_board` | 指标读数 | 读数编号、统计维度、统计窗口 |
| 抽检样本 | `sample_check` | 抽检样本 | 样本编号、关联批次、抽样比例 |
| 交接包 | `handoff_pack` | 交接包 | 交接编号、关联批次、目标系统 |
| 来源窗口迁移 | `window_migration` | 窗口迁移 | 迁移编号、来源窗口起止、时区、核验片段 |
| 落库回执 | `landing_receipt` | 落库回执 | 回执编号、关联批次、区间起止、时区 |
| 补采队列 | `backfill_queue` | 补采任务 | 任务编号、区间起止、时区、核验状态 |

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据采集、爬虫与RPA

## 来源窗口迁移（旧日标签 → 带时区的精确区间）

采集流水线历史上用 `[2026-11-21]` 这样的日标签表达采集范围。日标签只是标签，
不含钟点；跨午夜时它既不能说明 23:30→00:30 的真实区间，也不能据此补造时间戳。

迁移的处理原则：

- **只登记真实区间**：精确窗口的起止和时间区全部来自机器人补报。缺时间或缺时区
  一律拒绝（HTTP 400 / `WindowError`），不用旧标签反推、不猜填默认值。
- **原标签保留**：旧 `[2026-11-21]` 原样留在批次 `原始日标签` / `采集窗口` 和交接包
  `原始日标签` 上，迁移完成后仍可按原标签回查批次与已签收的交接记录。
- **按来源时区切段**：跨午夜窗口按上报时区的本地午夜切分，每个片段保留其时区。
- **逐段核验、可中断可恢复**：核实一段就写一张落库回执并推进补采任务；中断后恢复
  只处理未核验/失败片段，已核验片段不重复出回执。
- **同一补报只确认一次**：幂等键按 `(批次, 真实起止瞬时)` 计算，换一种等价时区写法
  重复补传也判为同一份补报，不会生成第二套回执或重复抵扣。
- **已交付片段抵扣、未知时间阻断**：片段完全落在原日标签当日且原批次已确认或存在
  已交接交接包时才判「已抵扣」；未核验/核实失败的区间在补采队列里保持阻断，未知时间
  既不猜填也不抵扣补采。
- **派发留痕且不重复**：每次派发尝试写入任务的 `派发记录` 与审计轨迹，首次被阻、
  恢复后的处理、重复派发、已抵扣都能区分；同一区间派发成功后再次调用返回 `duplicate`。

### 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/window_migration/report` | 机器人补报精确窗口；`verify`/`interrupt_after` 可直接触发分段核验与中断 |
| POST | `/api/window_migration/{id}/verify` | 核验迁移，可指定 `interrupt_after` 只核实前若干段 |
| POST | `/api/window_migration/{id}/resume` | 恢复中断迁移，只处理剩余/失败片段 |
| GET | `/api/window_migration/{id}/summary` | 回查迁移、原批次、已交付交接、回执与补采任务 |
| POST | `/api/backfill_queue/{id}/dispatch` | 按迁移核验结果派发补采区间 |
| GET | `/api/capture_batch?窗口起=…&窗口止=…` | 批次按精确区间重叠查询（历史日标签批次仍可按 `采集窗口` 查） |
| GET | `/api/backfill_queue?窗口起=…&窗口止=…` | 补采任务按精确区间重叠查询 |
| GET | `/api/handoff_pack?原始日标签=[2026-11-21]` | 按旧日标签回查交接记录 |

`窗口起`/`窗口止` 为左闭右开的查询边界，支持带偏移的 ISO 时间；没有精确窗口的
历史批次不会被区间查询命中，也不会被赋予臆造的时间。
