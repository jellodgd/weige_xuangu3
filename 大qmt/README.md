# 大 QMT 内置策略版

本目录现在是标准版大 QMT 的内置 Python 策略版本，不再是外部
`xtquant` / MiniQMT 脚本，也没有 Qt 启动器。

## 使用方式

1. 打开标准版大 QMT 客户端并登录。
2. 进入策略开发 / 模型管理，新建 Python 模型。
3. 将 `jingjia_filter.py` 的完整内容粘贴到 QMT 内置策略编辑器。
4. 在模型交易界面运行该模型，选择股票账号和实盘/模拟运行模式。
5. 将代码顶部的示例账号替换为实际账号：

```python
ACCOUNT_ID = "YOUR_ACCOUNT_ID"
```

如实际运行账号不同，需要在粘贴到 QMT 前修改该值。

## 当前策略流程

- `init(ContextInfo)` 设置交易账号并注册定时任务；
- `09:00:00` 加载沪深 A 股主板普通股并下载近 30 天日线；
- `09:15:05` 复用 09:00 股票池，读取缓存日线，准备昨收价、涨停价和 T-3 收盘价；
- `09:16:00` 至 `09:18:00` 轮询 precheck；
- `09:20:03`、`09:21:00`、`09:22:00`、`09:23:00`、`09:24:00`、`09:25:05` 分别采集 a～f；
- `09:26:35` 执行筛选；
- `09:27:00` 用 `passorder()` 下单；
- `10:00:00` 用 `get_trade_detail_data()` 查询委托，用 `cancel()` 撤掉未完成订单。

## 与 MiniQMT 版的区别

本文件不再使用：

```python
xtdata.connect()
xttrader.XtQuantTrader
order_stock()
cancel_order_stock()
```

而是使用大 QMT 内置接口：

```python
ContextInfo.schedule_run()
ContextInfo.get_stock_list_in_sector()
ContextInfo.get_full_tick()
ContextInfo.get_market_data_ex()
passorder()
get_trade_detail_data()
cancel()
```

## 注意

QMT 内置策略运行在客户端单线程环境中，代码不能写 `while True`、`sleep`
等阻塞逻辑。因此本版本用 QMT 定时器轮询采样窗口。

如果 `download_history_data()` 在当前券商 QMT 版本中不可用，请先在 QMT
菜单的数据管理中批量下载日线数据，再运行策略。

如果模型在交易日 `09:00:00` 之后、`09:15:05` 之前启动，代码会立即补跑一次
日线下载；如果在 `09:15:05` 之后才启动，则当天的 prepare 定时任务已经错过，
需要重新确认运行时机。
