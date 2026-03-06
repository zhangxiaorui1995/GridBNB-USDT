import asyncio
import logging
import traceback
import platform
import sys
from trader import GridTrader
from helpers import LogConfig, send_pushplus_message
from web_server import start_web_server
from exchange_client import ExchangeClient
from config import TradingConfig

# 在Windows平台上设置SelectorEventLoop
if platform.system() == 'Windows':
    import asyncio
    # 在Windows平台上强制使用SelectorEventLoop
    if sys.version_info[0] == 3 and sys.version_info[1] >= 8:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        logging.info("已设置Windows SelectorEventLoop策略")

async def _run_web_server_with_restart(trader, max_retries=5):
    """包装 web server，崩溃后自动重启，最多重试 max_retries 次"""
    retries = 0
    while retries < max_retries:
        try:
            await start_web_server(trader)
        except Exception as e:
            retries += 1
            logging.error(f"Web服务器崩溃（第{retries}次），{10}秒后重启: {e}")
            if retries < max_retries:
                await asyncio.sleep(10)
            else:
                logging.error("Web服务器达到最大重启次数，停止重启（交易循环继续运行）")
                return


async def main():
    try:
        # 初始化统一日志配置
        LogConfig.setup_logger()
        logging.info("="*50)
        logging.info("网格交易系统启动")
        logging.info("="*50)
        
        # 创建交易所客户端和配置实例
        exchange = ExchangeClient()
        config = TradingConfig()
        
        # 使用正确的参数初始化交易器
        trader = GridTrader(exchange, config)
        
        # 初始化交易器
        await trader.initialize()
        
        # 启动Web服务器（单独监控，崩溃后自动重启，不影响交易循环）
        web_server_task = asyncio.create_task(_run_web_server_with_restart(trader))
        
        # 启动交易循环（核心任务，异常会向上抛出）
        trading_task = asyncio.create_task(trader.main_loop())
        
        # 两个任务独立运行：trading_task 崩溃时通知并退出，web_server 崩溃时自动重启
        done, pending = await asyncio.wait(
            [web_server_task, trading_task],
            return_when=asyncio.FIRST_EXCEPTION
        )
        
        for task in done:
            if task.exception():
                logging.error(f"关键任务异常退出: {task.exception()}")
                send_pushplus_message(f"关键任务异常退出: {task.exception()}", "致命错误")
        
        for task in pending:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
    except Exception as e:
        error_msg = f"启动失败: {str(e)}\n{traceback.format_exc()}"
        logging.error(error_msg)
        send_pushplus_message(error_msg, "致命错误")
        
    finally:
        if 'trader' in locals():
            try:
                await trader.exchange.close()
                logging.info("交易所连接已关闭")
            except Exception as e:
                logging.error(f"关闭连接时发生错误: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main()) 