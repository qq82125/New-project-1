# Global IVD Morning Briefing Automation

一个面向 IVD 行业情报的自动化项目：按固定结构生成全球 IVD 晨报，并通过 iCloud SMTP 发送邮件，同时提供 GitHub Actions 云端兜底补发能力。

## Key Features

- 每日生成并发送《全球 IVD 晨报》（支持固定主题格式）。
- 本机 SMTP 发送脚本（iCloud SMTP，支持 UTF-8 正文）。
- GitHub Actions 云端兜底：检测当天是否已发送，未发送则自动补发。
- 自动化同步工具：提供本地 `git` 自动同步与 post-commit 自动推送能力。
- 配套部署文档，便于快速落地到个人仓库。

## Tech Stack

- `Bash`（自动化脚本）
- `Python 3`（云端兜底发送逻辑）
- `GitHub Actions`（定时任务与手动触发）
- `iCloud SMTP/IMAP`（邮件发送与“已发送”检测）
- `Git`（版本管理与同步）

## Getting Started

### 1. Clone 项目

```bash
git clone https://github.com/qq82125/New-project-1.git
cd New-project-1
```

### 2. 安装依赖

本项目核心脚本使用 Python 标准库，无强制第三方依赖。  
如需使用脚本化方式调用 GitHub Secrets 加密（高级用法），可安装：

```bash
pip install pynacl
```

### 3. 运行项目

1) 配置本机 SMTP 环境（示例 `.mail.env`）：

```bash
SMTP_HOST=smtp.mail.me.com
SMTP_PORT=587
SMTP_USER=your_mail@me.com
SMTP_PASS=your_app_password
SMTP_FROM=your_mail@me.com
SMTP_FROM_NAME=全球IVD晨报
```

2) 发送测试邮件：

```bash
./send_mail_icloud.sh qq82125@gmail.com "全球IVD晨报 - 2026-02-13（测试）" ./ivd_morning_2026-02-13.txt
```

3) 启用云端兜底（GitHub Actions）：
- 参考 `CLOUD_BACKUP_SETUP.md` 配置 Secrets。
- 在 Actions 中运行 `IVD Cloud Backup Mail` 做一次验证。

## Usage Example

### 命令行发送示例

```bash
./send_mail_icloud.sh \
  qq82125@gmail.com \
  "全球IVD晨报 - 2026-02-13" \
  ./ivd_morning_2026-02-13.txt
```

### 云端兜底逻辑（简述）

1. 每天 08:40（北京时间）触发 GitHub Actions。  
2. 先检查 iCloud「已发送」是否存在当日主题邮件。  
3. 若不存在，则自动补发到目标邮箱。  

## Contributing

欢迎提交 Issue 和 Pull Request。建议先描述问题背景与预期行为，再提交最小可复现改动，便于快速评审和合并。

## License

This project is licensed under the MIT License.
