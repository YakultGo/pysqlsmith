# MySQL SQL Generator

这个目录是一套独立运行的 MySQL SQL fuzzing 项目。

它的核心流程是：

1. `main.py` 用 `argparse` 接收数据库参数
2. `runtime.py` 建立一次 fuzzing session
3. `schema.py` 从 MySQL 里加载表、列、约束和内建能力
4. `grammar.py` / `expr.py` 生成 AST
5. `dut.py` 执行 SQL，收集错误和统计信息

如果你第一次看代码，最建议先打开 `main.py`。

## 安装依赖

推荐在仓库根目录创建虚拟环境：

```bash
python3 -m venv .venv
source .venv/bin/activate
```

然后安装 MySQL 这套项目自己的依赖：

```bash
python -m pip install -r pysqlsmith/mysql/requirements.txt
```

当前依赖很少：

- `pymysql`
- `cryptography`

## 怎么运行

最直接的运行方式就是从仓库根目录执行：

```bash
python pysqlsmith/mysql/main.py \
  --host 127.0.0.1 \
  --port 3306 \
  --user root \
  --password your_password \
  --dbname test \
  --max-queries 10 \
  --verbose
```

只生成 SQL、不执行：

```bash
python pysqlsmith/mysql/main.py \
  --host 127.0.0.1 \
  --port 3306 \
  --user root \
  --password your_password \
  --dbname test \
  --dry-run \
  --max-queries 10
```

只跑只读查询：

```bash
python pysqlsmith/mysql/main.py \
  --host 127.0.0.1 \
  --port 3306 \
  --user root \
  --password your_password \
  --dbname test \
  --select \
  --max-queries 10 \
  --verbose
```

查看全部参数：

```bash
python pysqlsmith/mysql/main.py --help
```

## 参数说明

- `--host`
  MySQL 主机地址
- `--port`
  MySQL 端口
- `--user`
  MySQL 用户名
- `--password`
  MySQL 密码
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
  尽量避开系统 catalog 表
- `--verbose`
  打印加载进度和执行统计

## 项目结构

- `main.py`
  项目入口，定义 CLI 参数和 `RunConfig`
- `runtime.py`
  主运行循环，负责调度 schema、生成器、执行器和统计
- `schema.py`
  加载 MySQL 的表、列、约束，以及生成器依赖的内建函数/算子信息
- `schema_base.py`
  schema 层的基础索引逻辑
- `relmodel.py`
  表、列、类型、scope 等核心数据模型
- `prod.py`
  AST 生成节点的基础类
- `grammar.py`
  语句级 AST，例如 `SELECT`、`INSERT`、`UPDATE`、`DELETE`
- `expr.py`
  表达式级 AST，例如常量、列引用、函数调用、子查询、布尔表达式
- `dut.py`
  真正把 SQL 发给 MySQL 执行
- `impedance.py`
  记录某些生成节点的失败/成功统计
- `logger.py`
  记录查询和错误输出
- `random_utils.py`
  所有随机选择和掷骰子工具
- `exceptions.py`
  项目内部异常定义

## 适合怎么用

这个项目更适合拿来做：

- 随机 SQL 生成
- MySQL parser / planner / executor 的回归测试
- DML 约束噪音观察
- 新语法节点或新生成策略实验

它不是 migration 工具，也不是 ORM。

## 运行前提

为了让生成器有足够的空间，目标库最好满足：

- 至少有一批普通业务表
- 表里有一些真实数据
- 不只是空 schema

如果库太空，生成器虽然还能跑，但可生成空间会明显变窄。
