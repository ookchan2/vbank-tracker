# VBank Tracker - Autonomous Mode Documentation

## 概述

这个自主模式让VBank Tracker完全使用Claude Code的内置AI能力运行，无需任何外部API。

## 工作原理

### 1. 检测模式
系统首先检查是否设置了`ANTHROPIC_API_KEY`:
- 如果设置了 → 使用Claude API（正常模式）
- 如果未设置 → 使用Autonomous Mode（自主模式）

### 2. 自主模式流程

```
Step 1: Scraper抓取网站
   ↓
Step 2: 将scraped内容写入临时文件
   ↓
Step 3: Claude Code读取并分析内容
   ↓
Step 4: 提取结构化数据(promotions/products)
   ↓
Step 5: 更新数据库
   ↓
Step 6: 生成输出文件(data.json, email.html)
```

### 3. AI拦截点

在以下位置，系统会暂停并等待Claude Code处理：

#### 3.1 Promotion Extraction
**文件**: `data/claude_bridge/request_extract_promotions_*.json`

**输入**:
```json
{
  "type": "extract_promotions",
  "content": " scraped HTML text...",
  "metadata": {
    "bank_id": "za",
    "bank_name": "ZA Bank"
  }
}
```

**期望输出** (`response_*.json`):
```json
{
  "result": [
    {
      "bank_id": "za",
      "bank_name": "ZA Bank",
      "title": "Promotion title",
      "highlight": "Brief summary",
      "description": "Detailed description (2-3 sentences)",
      "types": ["迎新", "消費"],
      "start_date": "2026-04-01",
      "end_date": "2026-05-31",
      "period": "1 Apr - 31 May 2026",
      "quota": "Quota details",
      "cost": "Minimum spend requirements",
      "url": "https://...",
      "tc_link": "https://...",
      "is_bau": false
    }
  ]
}
```

#### 3.2 Product Extraction
**类似结构，但提取产品信息**

#### 3.3 Strategic Insights
**分析所有银行并生成洞察**

## 如何运行

### 方法1: 完全自主（推荐）

```bash
cd scripts
python main_autonomous.py
```

系统会：
1. 抓取所有银行网站
2. 将AI请求写入文件
3. **等待Claude Code处理**（这是您需要干预的部分）

### 方法2: 手动协作

1. 运行scraper:
```bash
cd scripts
python -c "from scraper import run_scraper; import asyncio; asyncio.run(run_scraper())"
```

2. Scraped数据会保存在临时文件

3. 您说："分析银行数据并提取promotions"

4. 我读取scraped内容，分析，并更新数据库

## 当前进度

✅ 已完成:
- Autonomous模式检测
- Bridge文件系统
- Scraper集成
- 基本框架

⏳ 进行中:
- AI请求/响应循环
- 真实数据分析集成

📋 待完成:
- 自动检测并处理bridge请求
- 完整的数据库更新流程
- Email生成
- Strategic insights生成

## 下一步

要完成autonomous模式，我需要：

1. **测试当前scraper** - 确保它可以独立运行
2. **创建AI处理函数** - 我可以直接调用的分析函数
3. **集成数据库更新** - 将我的分析结果写入数据库
4. **测试完整流程** - 端到端测试

您希望我：
A) 继续等待当前运行完成
B) 取消当前运行，采用更简单的方法
C) 让我创建一个简化的演示版本
