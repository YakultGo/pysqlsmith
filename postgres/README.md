# PostgreSQL SQL Generator

这个目录是一套独立运行的 PostgreSQL SQL fuzzing 项目。

它的核心流程是：

1. `main.py` 用 `argparse` 接收数据库参数
2. `runtime.py` 建立一次 fuzzing session
3. `schema.py` 从 PostgreSQL 里加载类型、表、列、约束、算子、函数和聚合
4. `grammar.py` / `expr.py` 生成 AST
5. `dut.py` 执行 SQL，收集错误和统计信息

如果你第一次看代码，最建议先打开 `main.py`，然后看 `runtime.py` 和 `schema.py`。

## 安装依赖

推荐在仓库根目录创建虚拟环境：

```bash
python3 -m venv .venv
source .venv/bin/activate
```

然后安装 PostgreSQL 这套项目自己的依赖：

```bash
python -m pip install -r pysqlsmith/postgres/requirements.txt
```

当前主要依赖是：

- `psycopg[binary]`

## 怎么运行

最直接的运行方式就是从仓库根目录执行：

```bash
python pysqlsmith/postgres/main.py \
  --host 127.0.0.1 \
  --port 5432 \
  --user postgres \
  --password your_password \
  --dbname test \
  --max-queries 10 \
  --verbose
```

只生成 SQL、不执行：

```bash
python pysqlsmith/postgres/main.py \
  --host 127.0.0.1 \
  --port 5432 \
  --user postgres \
  --password your_password \
  --dbname test \
  --dry-run \
  --max-queries 10
```

只跑只读查询：

```bash
python pysqlsmith/postgres/main.py \
  --host 127.0.0.1 \
  --port 5432 \
  --user postgres \
  --password your_password \
  --dbname test \
  --select \
  --max-queries 10 \
  --verbose
```

更稳一点的回归起点通常是：

```bash
python pysqlsmith/postgres/main.py \
  --host 127.0.0.1 \
  --port 5432 \
  --user postgres \
  --password your_password \
  --dbname test \
  --exclude-catalog \
  --select \
  --max-queries 10 \
  --verbose
```

查看全部参数：

```bash
python pysqlsmith/postgres/main.py --help
```

## 参数说明

- `--host`
  PostgreSQL 主机地址
- `--port`
  PostgreSQL 端口
- `--user`
  PostgreSQL 用户名
- `--password`
  PostgreSQL 密码
- `--dbname`
  目标数据库名
- `--seed`
  固定随机种子，方便复现问题
- `--max-queries`
  生成或执行多少条 SQL 后退出
- `--select`
  只生成 `SELECT`
- `--dry-run`
  只打印 SQL，不真正执行
- `--dump-all-queries`
  把所有 SQL 追加写入 `queries.log`
- `--exclude-catalog`
  尽量避开系统 catalog relation，减少环境噪音
- `--verbose`
  打印加载进度和执行统计

## 项目结构

- `main.py`
  项目入口，定义 CLI 参数和 `RunConfig`
- `runtime.py`
  主运行循环，负责调度 schema、生成器、执行器和统计
- `schema.py`
  PostgreSQL 最重的 backend 文件，负责类型系统、约束、内建对象过滤和索引
- `schema_base.py`
  schema 层的基础索引逻辑
- `relmodel.py`
  表、列、类型、scope 等核心数据模型
- `prod.py`
  AST 生成节点的基础类
- `grammar.py`
  语句级 AST，例如 `SELECT`、`INSERT`、`UPDATE`、`DELETE`、`MERGE`
- `expr.py`
  表达式级 AST，例如常量、列引用、函数调用、窗口函数、子查询、布尔表达式
- `dut.py`
  真正把 SQL 发给 PostgreSQL 执行
- `impedance.py`
  记录某些生成节点的失败/成功统计
- `logger.py`
  记录查询和错误输出
- `random_utils.py`
  所有随机选择和掷骰子工具
- `exceptions.py`
  项目内部异常定义

## 这个项目重点解决什么

PostgreSQL 这套生成器重点在这些事情上：

- 随机 SQL 生成
- PostgreSQL parser / planner / executor 回归
- 类型系统和多态函数的组合覆盖
- DML 噪音控制
- catalog / builtin 相关语义探索

它不是 migration 工具，也不是 ORM。

## `--exclude-catalog` 是什么

这个参数不是“完全不用系统对象”，而是：

- 尽量不从 `pg_catalog` / `information_schema` 挑 relation
- 但仍然会加载类型、函数、算子、聚合，因为表达式生成离不开这些对象
- 配合 PostgreSQL backend 里的对象过滤逻辑，把明显依赖权限、实例状态或 session 状态的内建对象尽量剔掉

如果你是第一次回归，建议先开着它。

## 运行前提

为了让 PostgreSQL 这套生成器发挥出来，目标库最好满足：

- 至少有一批普通业务表
- 表里有一些真实数据
- 不只是空 schema

库太空时，类型和 relation 组合空间会变小，很多生成路径就走不出来。
