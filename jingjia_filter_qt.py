# -*- coding: utf-8 -*-
"""Qt launcher/config editor for jingjia_filter.py.

这个界面只负责两件事：
1. 编辑 qmt_accounts_config.json 里的启动账号和 MiniQMT 根目录；
2. 用当前配置启动 jingjia_filter.py，并把控制台输出显示到窗口里。
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

from PySide6.QtCore import QProcess, QProcessEnvironment, Qt
from PySide6.QtGui import QTextCursor
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)


BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "qmt_accounts_config.json"
STRATEGY_PATH = BASE_DIR / "jingjia_filter.py"


def normalize_account(item: dict[str, Any]) -> dict[str, str]:
    return {
        "account_id": str(item.get("account_id") or "").strip(),
        "account_mode": str(item.get("account_mode") or "模拟账户").strip(),
        "qmt_root": str(item.get("qmt_root") or "").strip(),
    }


def load_config() -> tuple[str, list[dict[str, str]]]:
    if not CONFIG_PATH.exists():
        return "", []

    data = json.loads(CONFIG_PATH.read_text(encoding="utf-8-sig"))
    active_account_id = str(data.get("active_account_id") or "").strip()
    raw_accounts = data.get("accounts") or []
    if not isinstance(raw_accounts, list):
        raise ValueError("accounts 必须是列表")

    accounts: list[dict[str, str]] = []
    for item in raw_accounts:
        if isinstance(item, dict):
            account = normalize_account(item)
            if account["account_id"] or account["qmt_root"]:
                accounts.append(account)
    return active_account_id, accounts


def save_config(active_account_id: str, accounts: list[dict[str, str]]) -> None:
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "active_account_id": active_account_id,
        "accounts": accounts,
    }
    CONFIG_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + os.linesep,
        encoding="utf-8",
    )


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("竞价策略配置启动器")
        self.resize(1040, 700)
        self.process: QProcess | None = None
        self.accounts: list[dict[str, str]] = []

        self.account_select = QComboBox()
        self.account_select.currentIndexChanged.connect(self.on_account_changed)

        self.account_id_edit = QLineEdit()
        self.account_id_edit.setPlaceholderText("例如：12345678")

        self.account_mode_select = QComboBox()
        self.account_mode_select.addItems(["实盘账户", "模拟账户"])

        self.qmt_root_edit = QLineEdit()
        self.qmt_root_edit.setPlaceholderText("例如：D:\\GUOJIN_QMT_MONI")

        self.browse_button = QPushButton("选择目录")
        self.browse_button.clicked.connect(self.browse_qmt_root)

        self.start_button = QPushButton("启动策略")
        self.start_button.clicked.connect(self.start_strategy)

        self.download_daily_button = QPushButton("下载近30天数据")
        self.download_daily_button.clicked.connect(self.download_daily_data)

        self.check_daily_ready_button = QPushButton("检查最近三个交易日数据")
        self.check_daily_ready_button.clicked.connect(self.check_daily_ready)

        self.stop_button = QPushButton("停止策略进程")
        self.stop_button.setEnabled(False)
        self.stop_button.clicked.connect(self.stop_strategy)

        self.extra_args_edit = QLineEdit()
        self.extra_args_edit.setPlaceholderText(
            "可选，例如：--once、--no-strict-root-check 或 --no-download-daily"
        )

        self.first_rise_select = QComboBox()
        for pct in range(5, 10):
            self.first_rise_select.addItem(f"{pct}%以上", str(pct))
        self.first_rise_select.addItem("涨停", "limit_up")

        self.max_order_stocks_spin = QSpinBox()
        self.max_order_stocks_spin.setRange(1, 100)
        self.max_order_stocks_spin.setValue(2)
        self.max_order_stocks_spin.setSingleStep(1)

        self.single_stock_cap_select = QComboBox()
        for pct in range(20, 101, 10):
            label = f"{pct}%"
            if pct == 100:
                label += "（无上限）"
            self.single_stock_cap_select.addItem(label, str(pct))
        self.single_stock_cap_select.setCurrentText("50%")

        self.prior_max_drop_check = QCheckBox("启用T-2/T-1最大跌幅过滤")
        self.prior_max_drop_check.setChecked(False)

        self.prior_max_drop_select = QComboBox()
        for pct in (-9, -8, -7, -6, -5):
            self.prior_max_drop_select.addItem(f"{pct}%以上", str(pct))
        self.prior_max_drop_select.setCurrentText("-5%以上")
        self.prior_max_drop_select.setEnabled(False)
        self.prior_max_drop_check.stateChanged.connect(
            lambda state: self.prior_max_drop_select.setEnabled(bool(state))
        )

        self.exclude_final_price_below_5_check = QCheckBox("当前价格必须>=5元")
        self.exclude_final_price_below_5_check.setChecked(True)

        self.prior_t1_rise_check = QCheckBox("T-1当日涨幅必须<=7%")
        self.prior_t1_rise_check.setChecked(True)

        self.download_on_start_check = QCheckBox("在9:00下载最近30天数据")
        self.download_on_start_check.setChecked(True)

        self.status_label = QLabel("")
        self.status_label.setTextInteractionFlags(Qt.TextSelectableByMouse)

        self.output = QPlainTextEdit()
        self.output.setReadOnly(True)
        self.output.setLineWrapMode(QPlainTextEdit.NoWrap)

        self.build_layout()
        self.reload_config()

    def build_layout(self) -> None:
        account_form = QFormLayout()
        account_form.addRow("配置文件", QLabel(str(CONFIG_PATH)))
        account_form.addRow("当前账号", self.account_select)
        account_form.addRow("账号", self.account_id_edit)
        account_form.addRow("账户类型", self.account_mode_select)

        qmt_row = QHBoxLayout()
        qmt_row.addWidget(self.qmt_root_edit, 1)
        qmt_row.addWidget(self.browse_button)
        account_form.addRow("MiniQMT目录", qmt_row)

        account_group = QGroupBox("账号与 MiniQMT 配置")
        account_group.setLayout(account_form)

        launch_form = QFormLayout()
        launch_form.addRow("策略文件", QLabel(str(STRATEGY_PATH)))

        option_form = QFormLayout()
        option_form.addRow("a点涨幅条件", self.first_rise_select)
        option_form.addRow("最大下单股票数", self.max_order_stocks_spin)
        option_form.addRow("单只预算上限", self.single_stock_cap_select)
        option_form.addRow("价格过滤", self.exclude_final_price_below_5_check)
        option_form.addRow("T-1涨幅过滤", self.prior_t1_rise_check)
        prior_drop_row = QHBoxLayout()
        prior_drop_row.addWidget(self.prior_max_drop_check)
        prior_drop_row.addWidget(self.prior_max_drop_select)
        prior_drop_row.addStretch(1)
        option_form.addRow("前两日跌幅过滤", prior_drop_row)

        option_group = QGroupBox("可调整选项")
        option_group.setStyleSheet(
            "QGroupBox {"
            "border: 1px solid #707070;"
            "border-radius: 4px;"
            "margin-top: 10px;"
            "font-weight: 600;"
            "}"
            "QGroupBox::title {"
            "subcontrol-origin: margin;"
            "left: 10px;"
            "padding: 0 4px;"
            "}"
        )
        option_group.setLayout(option_form)
        launch_form.addRow(option_group)

        launch_form.addRow("启动前下载", self.download_on_start_check)
        launch_form.addRow("额外启动参数", self.extra_args_edit)

        utility_buttons = QHBoxLayout()
        utility_buttons.addWidget(self.download_daily_button)
        utility_buttons.addWidget(self.check_daily_ready_button)
        utility_buttons.addStretch(1)
        launch_form.addRow("", utility_buttons)

        launch_buttons = QHBoxLayout()
        launch_buttons.addWidget(self.start_button)
        launch_buttons.addWidget(self.stop_button)
        launch_buttons.addStretch(1)
        launch_form.addRow("", launch_buttons)

        launch_group = QGroupBox("启动")
        launch_group.setLayout(launch_form)

        layout = QVBoxLayout()
        layout.addWidget(account_group)
        layout.addWidget(launch_group)
        layout.addWidget(self.status_label)
        layout.addWidget(self.output, 1)

        root = QWidget()
        root.setLayout(layout)
        self.setCentralWidget(root)

    def append_output(self, text: str) -> None:
        self.output.moveCursor(QTextCursor.End)
        self.output.insertPlainText(text)
        self.output.moveCursor(QTextCursor.End)

    def reload_config(self) -> None:
        try:
            active_account_id, accounts = load_config()
        except Exception as exc:  # noqa: BLE001 - surface config errors in UI
            QMessageBox.critical(self, "配置读取失败", str(exc))
            active_account_id, accounts = "", []

        self.accounts = accounts
        self.account_select.blockSignals(True)
        self.account_select.clear()
        for account in self.accounts:
            label = account["account_id"]
            if account["account_id"] == active_account_id:
                label = f"{label}（当前）"
            if account.get("account_mode"):
                label = f"{label} - {account['account_mode']}"
            self.account_select.addItem(label, account["account_id"])
        self.account_select.blockSignals(False)

        index = 0
        if active_account_id:
            for i, account in enumerate(self.accounts):
                if account["account_id"] == active_account_id:
                    index = i
                    break
        if self.accounts:
            self.account_select.setCurrentIndex(index)
            self.show_account(index)
        else:
            self.account_id_edit.clear()
            self.account_mode_select.setCurrentText("模拟账户")
            self.qmt_root_edit.clear()

        self.update_status()

    def update_status(self) -> None:
        account = self.current_account()
        if not account:
            self.status_label.setText("当前没有可用账号，请检查 qmt_accounts_config.json。")
            return
        userdata = Path(account["qmt_root"]) / "userdata_mini"
        userdata_status = "存在" if userdata.is_dir() else "未找到"
        self.status_label.setText(
            f"当前将使用账号 {account['account_id']}（{account.get('account_mode', '')}）；"
            f"MiniQMT目录：{account['qmt_root']}；"
            f"userdata_mini：{userdata_status}"
        )

    def show_account(self, index: int) -> None:
        if index < 0 or index >= len(self.accounts):
            return
        account = self.accounts[index]
        self.account_id_edit.setText(account["account_id"])
        self.account_mode_select.setCurrentText(account.get("account_mode") or "模拟账户")
        self.qmt_root_edit.setText(account["qmt_root"])
        self.update_status()

    def on_account_changed(self, index: int) -> None:
        self.show_account(index)

    def current_account(self) -> dict[str, str] | None:
        index = self.account_select.currentIndex()
        if index < 0 or index >= len(self.accounts):
            return None
        return self.accounts[index]

    def current_form_account(self) -> dict[str, str]:
        return {
            "account_id": self.account_id_edit.text().strip(),
            "account_mode": self.account_mode_select.currentText().strip(),
            "qmt_root": self.qmt_root_edit.text().strip(),
        }

    def validate_form_account(self) -> dict[str, str] | None:
        account = self.current_form_account()
        if not account["account_id"]:
            QMessageBox.warning(self, "账号不能为空", "请填写账号。")
            return None
        if not account["account_id"].isdigit():
            QMessageBox.warning(self, "账号格式错误", "账号应为数字。")
            return None
        if account["account_mode"] not in {"实盘账户", "模拟账户"}:
            QMessageBox.warning(self, "账户类型错误", "账户类型必须是实盘账户或模拟账户。")
            return None
        if not account["qmt_root"]:
            QMessageBox.warning(self, "MiniQMT目录不能为空", "请填写或选择 MiniQMT 目录。")
            return None
        return account

    def browse_qmt_root(self) -> None:
        current = self.qmt_root_edit.text().strip()
        start_dir = current if current and Path(current).exists() else str(BASE_DIR)
        selected = QFileDialog.getExistingDirectory(self, "选择 MiniQMT 根目录", start_dir)
        if selected:
            self.qmt_root_edit.setText(selected)
            self.update_status()

    def refresh_account_select(self, active_account_id: str) -> None:
        self.account_select.blockSignals(True)
        self.account_select.clear()
        for account in self.accounts:
            label = account["account_id"]
            if account["account_id"] == active_account_id:
                label = f"{label}（当前）"
            if account.get("account_mode"):
                label = f"{label} - {account['account_mode']}"
            self.account_select.addItem(label, account["account_id"])
        self.account_select.blockSignals(False)

        if not self.accounts:
            self.account_id_edit.clear()
            self.account_mode_select.setCurrentText("模拟账户")
            self.qmt_root_edit.clear()
            self.update_status()
            return

        for index, account in enumerate(self.accounts):
            if account["account_id"] == active_account_id:
                self.account_select.setCurrentIndex(index)
                self.show_account(index)
                break

    def save_current_config(self) -> bool:
        account = self.validate_form_account()
        if account is None:
            return False

        found = False
        for index, existing in enumerate(self.accounts):
            if existing["account_id"] == account["account_id"]:
                self.accounts[index] = account
                found = True
                break
        if not found:
            self.accounts.append(account)

        try:
            save_config(account["account_id"], self.accounts)
        except Exception as exc:  # noqa: BLE001 - surface write errors in UI
            QMessageBox.critical(self, "保存失败", str(exc))
            return False

        self.refresh_account_select(account["account_id"])
        self.append_output(
            f"[GUI] 已保存配置：account={account['account_id']}, "
            f"mode={account['account_mode']}, qmt_root={account['qmt_root']}{os.linesep}"
        )
        return True

    def start_strategy(self) -> None:
        if self.process is not None and self.process.state() != QProcess.NotRunning:
            QMessageBox.information(self, "策略运行中", "当前策略进程已经在运行。")
            return
        if self.validate_form_account() is None:
            return
        if not STRATEGY_PATH.exists():
            QMessageBox.critical(self, "策略文件不存在", str(STRATEGY_PATH))
            return

        args = [
            str(STRATEGY_PATH),
            "--config",
            str(CONFIG_PATH),
            "--max-order-stocks",
            str(int(self.max_order_stocks_spin.value())),
            "--single-stock-asset-ratio-pct",
            str(self.single_stock_cap_select.currentData() or "50"),
        ]
        first_rise_rule = self.first_rise_select.currentData()
        if first_rise_rule == "limit_up":
            args.append("--first-rise-limit-up")
        else:
            args.extend(["--first-min-rise-pct", str(first_rise_rule or "5")])
        if self.prior_max_drop_check.isChecked():
            args.extend(
                [
                    "--prior-max-drop-check",
                    "--prior-max-drop-threshold-pct",
                    str(self.prior_max_drop_select.currentData() or "-5"),
                ]
            )
        if self.exclude_final_price_below_5_check.isChecked():
            args.append("--exclude-final-price-below-5")
        if self.prior_t1_rise_check.isChecked():
            args.extend(["--prior-t1-rise-check", "--prior-t1-max-rise-pct", "7"])
        if self.download_on_start_check.isChecked():
            args.extend(["--download-daily", "--daily-download-time", "09:00:00"])
        else:
            args.append("--no-download-daily")
        extra = self.extra_args_edit.text().strip()
        if extra:
            args.extend(extra.split())

        self.start_process(args, "启动策略")

    def download_daily_data(self) -> None:
        if self.process is not None and self.process.state() != QProcess.NotRunning:
            QMessageBox.information(self, "进程运行中", "当前已有进程在运行。")
            return
        if not self.save_current_config():
            return
        args = [
            str(STRATEGY_PATH),
            "--config",
            str(CONFIG_PATH),
            "--download-daily-only",
            "--daily-lookback-days",
            "30",
        ]
        self.start_process(args, "下载近30天数据")

    def check_daily_ready(self) -> None:
        if self.process is not None and self.process.state() != QProcess.NotRunning:
            QMessageBox.information(self, "进程运行中", "当前已有进程在运行。")
            return
        if not self.save_current_config():
            return
        args = [
            str(STRATEGY_PATH),
            "--config",
            str(CONFIG_PATH),
            "--check-daily-ready",
            "--daily-lookback-days",
            "30",
        ]
        self.start_process(args, "检查最近三个交易日数据")

    def start_process(self, args: list[str], label: str) -> None:
        if not STRATEGY_PATH.exists():
            QMessageBox.critical(self, "策略文件不存在", str(STRATEGY_PATH))
            return
        self.process = QProcess(self)
        self.process.setWorkingDirectory(str(BASE_DIR))
        python_executable = sys.executable
        self.process.setProgram(python_executable)
        self.process.setArguments(args)
        self.process.setProcessChannelMode(QProcess.MergedChannels)
        environment = QProcessEnvironment.systemEnvironment()
        environment.insert("PYTHONUTF8", "1")
        environment.insert("PYTHONIOENCODING", "utf-8")
        self.process.setProcessEnvironment(environment)
        self.process.readyReadStandardOutput.connect(self.read_process_output)
        self.process.finished.connect(self.on_process_finished)
        self.process.errorOccurred.connect(self.on_process_error)

        self.append_output(
            f"[GUI] {label}：{python_executable} {' '.join(args)}{os.linesep}"
        )
        self.start_button.setEnabled(False)
        self.download_daily_button.setEnabled(False)
        self.check_daily_ready_button.setEnabled(False)
        self.stop_button.setEnabled(True)
        self.process.start()

    def read_process_output(self) -> None:
        if self.process is None:
            return
        data = self.process.readAllStandardOutput().data()
        if data:
            self.append_output(data.decode("utf-8", errors="replace"))

    def on_process_finished(self, exit_code: int, exit_status: QProcess.ExitStatus) -> None:
        status = "正常退出" if exit_status == QProcess.NormalExit else "异常退出"
        self.append_output(f"[GUI] 策略进程{status}，exit_code={exit_code}{os.linesep}")
        self.start_button.setEnabled(True)
        self.download_daily_button.setEnabled(True)
        self.check_daily_ready_button.setEnabled(True)
        self.stop_button.setEnabled(False)

    def on_process_error(self, error: QProcess.ProcessError) -> None:
        self.append_output(f"[GUI] 策略进程错误：{error}{os.linesep}")
        self.start_button.setEnabled(True)
        self.download_daily_button.setEnabled(True)
        self.check_daily_ready_button.setEnabled(True)
        self.stop_button.setEnabled(False)

    def stop_strategy(self) -> None:
        if self.process is None or self.process.state() == QProcess.NotRunning:
            return
        reply = QMessageBox.question(
            self,
            "确认停止",
            "确定要停止当前策略进程吗？如果正在接近下单时间，请谨慎操作。",
        )
        if reply != QMessageBox.Yes:
            return
        self.process.terminate()
        if not self.process.waitForFinished(3000):
            self.process.kill()

    def closeEvent(self, event) -> None:  # type: ignore[override]
        if self.process is not None and self.process.state() != QProcess.NotRunning:
            QMessageBox.warning(self, "策略运行中", "策略运行时请先停止进程，再关闭窗口。")
            event.ignore()
            return
        event.accept()


def main() -> int:
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
