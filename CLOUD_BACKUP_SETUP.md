# 云端兜底发送（GitHub Actions）

目标：北京时间每天 `08:40` 运行云端兜底任务。  
逻辑：先查 iCloud「已发送」是否已有当天主题 `全球IVD晨报 - YYYY-MM-DD`；有则跳过，无则补发。

## 需要的文件
- `.github/workflows/ivd-cloud-backup.yml`
- `scripts/cloud_backup_send.py`

## GitHub Secrets
在仓库 `Settings -> Secrets and variables -> Actions` 新增：

1. `IVD_SMTP_USER`：`suguangyu@me.com`
2. `IVD_SMTP_PASS`：你的 Apple App 专用密码
3. `IVD_SMTP_FROM`：`suguangyu@me.com`
4. `IVD_TO_EMAIL`：`qq82125@gmail.com`
5. `IVD_IMAP_SENT_MAILBOX`：`Sent Messages`（可选，建议先填这个）

## 可选：让云端发送正式晨报正文
如果你希望云端补发时带完整正文，而不是兜底提示，可在仓库里每天提前生成：
- `reports/ivd_morning_YYYY-MM-DD.txt`

脚本会优先读取该文件作为邮件正文；找不到时发送兜底提示邮件。

## 手动测试
在 GitHub Actions 页面运行 `IVD Cloud Backup Mail` 的 `Run workflow`。

## 时区说明
工作流使用 `cron: 40 0 * * *`，对应北京时间 `08:40`（UTC+8）。
