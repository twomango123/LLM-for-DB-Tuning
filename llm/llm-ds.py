import json
import os
import asyncio
import aiohttp
import queue
import re
import ast
from datetime import datetime
import paramiko
from typing import Dict
from typing import TextIO
from enum import Enum
from openai import OpenAI

"""
DeepSeek Chat API integration

- API Key:   from env var `DEEPSEEK_API_KEY`
- Base URL:  https://api.deepseek.com
- SDK:       openai (OpenAI client)

保持对外函数签名与行为不变：`call_llm_api(session, prompt, background)` 仍返回
包含 `choices[0].message.content` 的 dict，以兼容现有调用方。
"""

api_key = "sk-b31d7d73e90c43888d28052469430d4f"

# OpenAI 兼容客户端（指向 DeepSeek 网关）
client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")

# 使用 DeepSeek 聊天模型
model = "deepseek-chat"

schema = {}

node_num = -1

row_num = {}

initial_data = {
    "model": model,
    "messages": [
        {"role": "system", "content": "You are a helpful assistant"},
        {"role": "user", "content": "数据库出现了一个新的查询模版: $query_template ; 该查询在当前分布键下$distributed_keys 的查询计划：$query_plan ；一些相似查询在不同分布键下的查询计划与实际执行效果:  $history ; 请给出你认为效果最好的分布键调整方案，和可能更好的分布键调整方案。要求: 1. 给出的方案不能与当前的方案完全相同。2.你的回答必须以{\\\"best solution\\\":{\\\"table_name1\\\":\\\"distribute_key\\\"},....};{\\\"better solution\\\":{\\\"table_name1\\\":\\\"distribute_key\\\"},....};{\\\"explain\\\":\\\"你的解释\\\"}的形式给出"}
    ],
    "stream": False
}


alt_key_request_template = \
"数据库出现了一个新的查询模版: $query_template ; ]\
该查询在当前分布键下$distributed_keys 的查询计划：$query_plan ,\
实际执行计划: $query_explain_analyze , 执行耗时为: $query_cost_time秒\
一些相似查询在不同分布键下的查询计划与实际执行效果:  $history ;\
请给出你认为效果最好的分布键调整方案，和可能更好的分布键调整方案。\
要求:1.你的回答中的分布键必须在对应表中存在(检查我发送给你的schema) \
2.你的回答必须以\"answer:{\"best solution\":{\"table_name1\":\"distribute_key\",....},\"better solution\":{\"table_name1\":\"distribute_key\",....},\"explain\":\"你的解释\"}\"的形式给出,不需要其他任何内容。\
请你在回答之前务必确定满足以上要求。\
"

alt_environment_request_template = \
"系统环境发生变化:\
节点数量变为:$node_num;\
schema变为:$schema;\
各表的行数变为:$row_num;\
"


class LLMRequestTypeEnum(Enum):
    EMPTY = "EMPTY"                     # 用于初始化
    INITIAL = "INITIAL"                 # 用于告诉LLM任务和schema和环境
    ALT_KEY = "ALT_KEY"                 # 询问分布键
    SELF_CHECK = "SELF_CHECK"           # 自检
    ALT_ENVIRONMENT = "ALT_ENVIRONMENT" # 更换环境
    STOP = "STOP"                       # 停止

class LLMRequestStateEnum(Enum):           
    EMPTY = "EMPTY"         # 用于初始化        
    INITIAL = "INITIAL"     # 只是在sequencer上生成了
    READY = "READY"         # 等待worker进行执行
    DOING = "DOING"         # worker正在执行
    DONE = "DONE"           # 已经执行完毕
    CHECKED = "CHECKED"     # 已经被检查过
    ERROR = "ERROR"         # 执行过程发生错误

# 定义用于传输LLM请求的结构体
class LLMRequestStruct:
    def __init__(self, type:LLMRequestTypeEnum = LLMRequestTypeEnum.EMPTY, id: int = -1, state: LLMRequestStateEnum = LLMRequestStateEnum.EMPTY,parameter = {},prompt = "" , result: str = ""):
        self.type = type
        self.id = id
        self.state = state
        self.parameter = parameter
        self.prompt = prompt
        self.result = result
    
    def __repr__(self):
        return f"LLMRequestStruct(type={self.type}, id={self.id}, state={self.state}, result={self.result})"

def parse_top_output(top_output: str) -> tuple:
    """
    从top命令的内容提取出CPU,内存,交换分区的利用率

    参数:
        top_output (str): 字符串形式的top命令的输出

    返回值:
        tuple: 一个包含三个float数值的tuple:
            - CPU 利用率 (%)
            - Memory 利用率 (%)
            - Swap 利用率 (%)
    """
    # 提取CPU利用率 (100 - idle percentage)
    cpu_usage = round(100.0 - float(re.search(r"(\d+\.\d+) id", top_output).group(1)), 2)

    # 提取内存利用率 (used / total * 100)
    mem_total = float(re.search(r"(\d+\.\d+) total", top_output).group(1))
    mem_used = float(re.search(r"(\d+\.\d+) used", top_output).group(1))
    mem_usage = round((mem_used / mem_total) * 100, 2)

    # 提取交换分区的利用率 (swap used / swap total * 100)
    swap_total_match = re.search(r"MiB Swap:\s+(\d+\.\d+) total", top_output)
    swap_used_match = re.search(r"MiB Swap:\s+\d+\.\d+ total,\s+\d+\.\d+ free,\s+(\d+\.\d+) used", top_output)

    if swap_total_match and swap_used_match:
        swap_total = float(swap_total_match.group(1))
        swap_used = float(swap_used_match.group(1))
        swap_usage = round((swap_used / swap_total) * 100, 2) if swap_total > 0 else 0.0
    else:
        swap_usage = 0.0

    return cpu_usage, mem_usage, swap_usage


def get_remote_usage(host: str, username: str, password: str) -> tuple[float, float, float, str]:
    """
    连接远程服务器并获取系统资源使用情况。

    参数:
        host : 远程服务器地址 (str)
        username : 连接服务器的用户名 (str)
        password : 用户的密码凭据 (str)

    返回:
        tuple: 包含以下四个元素:
            - CPU 利用率 (float)
            - 内存利用率 (float)
            - 交换区利用率 (float)
            - 错误信息 (str), 如果成功则为""，否则返回错误消息
    """
    try:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(host, username=username, password=password)
        
        stdin, stdout, stderr = client.exec_command("top -b -n 1 | head -n 5")
        top_output = stdout.read().decode()
        client.close()
        cpu_usage , mem_usage , swap_usage = parse_top_output(top_output)
        return cpu_usage , mem_usage , swap_usage , ""
    except Exception as e:
        return None , None , None , str(e)

def parse_string_to_dict(input_str : str) -> dict:
    """
    从llm发送的信息中解析出字典
    
    参数：
        input : llm返回的结果(str)
    返回:
        dict : 提示词要求的字典格式
    """
    answer_index = input_str.find("answer")
    if answer_index != -1:
        input_str = input_str[answer_index + len("answer"):]
    # 找到第一个 "{" 和最后一个 "}" 的位置
    start_index = input_str.find("{")
    end_index = input_str.rfind("}")
    if start_index != -1 and end_index != -1:
        content = input_str[start_index:end_index+1]
        content_cleaned = content.replace("\n", "").replace("\t", "").replace(";",",")
        dict = ast.literal_eval(content_cleaned)
        return dict
    else:
        return None

async def call_llm_api(session: aiohttp.ClientSession, prompt: str , background: str) -> dict:
    """
    发送异步请求调用 LLM API，并返回响应结果。

    参数:
        session : aiohttp 客户端会话，用于发送 HTTP 请求 (aiohttp.ClientSession)
        prompt : 用户输入的提示信息 (str)
        background : 用户输入的背景信息 (str)
    返回:
        dict : API 的 JSON 响应数据，包含模型生成的结果。

    异常:
        如果请求失败或返回非 JSON 响应，可能会引发相应的异常。
    """ 
    global api_key
    global model
    # 参考 DeepSeek 使用方式：一个 system + 一个 user 消息
    # 为兼容历史逻辑，将 background 与 prompt 合并为单条 user 内容
    user_content = prompt if not background else f"{background}\n{prompt}"
    messages = [
        {"role": "system", "content": "You are a helpful assistant"},
        {"role": "user", "content": user_content},
    ]

    # OpenAI SDK 为同步调用；在异步上下文中转移到线程池执行
    def _sync_call():
        return client.chat.completions.create(
            model=model,
            messages=messages,
            stream=False,
        )

    resp = await asyncio.to_thread(_sync_call)

    # 优先返回完整 dict（兼容 on_llm_done 的取值路径）
    try:
        return resp.model_dump()
    except Exception:
        # 退化为仅返回 content 的最小结构
        try:
            content = resp.choices[0].message.content
        except Exception:
            content = ""
        return {"choices": [{"message": {"content": content}}]}

def check_dist_keys_dict(dist_keys_dict:dict, schema:dict) -> bool:
    """
    检查分布键的字典是否符合要求

    参数:
        dist_keys_dict : 分布键的字典
        schema : 数据库的schema

    返回:
        bool : 是否符合要求
    """
    
    dist_keys_dict = {k.upper(): str(v).upper() for k, v in dist_keys_dict.items()}
    
    if not isinstance(schema, dict):
        print("check_dist_keys_dict: schema 不是字典")
        return False
    
    for table_name in dist_keys_dict.keys():
        # 确保 table_name 存在于 schema 中（表名区分大小写）
        if table_name not in schema and table_name.lower() not in schema:
            return False

        # 获取当前 table_name 对应的表的 schema 字典
        table_schema = schema.get(table_name) or schema.get(table_name.lower())

        # 检查 dist_keys_dict 中指定的键是否在该表的 schema 中
        if dist_keys_dict[table_name] not in table_schema and dist_keys_dict[table_name].lower() not in table_schema:
            if dist_keys_dict[table_name] != "REPLICATED":
                return False
    return True

async def on_llm_done(task: asyncio.Task, request:LLMRequestStruct , tos_queue:queue.Queue , f:TextIO , schema : dict) -> None:
    """
    处理完成的 LLM 任务结果的回调函数。

    参数:
        task : 已完成的异步任务对象 (asyncio.Task)
        request : LLM请求的结构体
        f : 用于输出日志的文件对象 (TextIO)
        tos_queue : 用于向scheduler传送消息的队列 (queue.Queue)
        schema : 数据库的schema
    返回:
        None

    异常处理:
        - 如果任务成功完成，则提取并打印 LLM 生成的内容。
        - 如果任务失败或被取消，则捕获异常并打印错误信息。
    """
    # try:
    response = task.result()
    # 提取 LLM 生成的 content
    content = response["choices"][0]["message"]["content"]
    request.state=LLMRequestStateEnum.DONE
    request.result = content
    if request.type == LLMRequestTypeEnum.ALT_KEY:
        temp_dict = parse_string_to_dict(request.result)
        print(f"temp_dict = {temp_dict}",file=f)
        f.flush()
        best_solution_dict = temp_dict["best solution"]
        better_solution_dict = temp_dict["better solution"]
        if isinstance(best_solution_dict, dict) == False or isinstance(better_solution_dict, dict) == False:
            print(f"解析LLM返回的信息时出现错误,返回的字典格式不正确",file=f)
        if check_dist_keys_dict(best_solution_dict, schema) == False:
            best_solution_dict = {}
        if check_dist_keys_dict(better_solution_dict, schema) == False:
            better_solution_dict = {}
        answer_rebuild = {"best solution":best_solution_dict,"better solution":better_solution_dict,"explain":temp_dict["explain"]}
        request.result = str(answer_rebuild)
        request.state = LLMRequestStateEnum.CHECKED
    print(f"接收到LLM返回的信息,放入request,request的内容为:{request}",file=f)
    f.flush()
    tos_queue.put(request)
    # except Exception as e:
    #     print(f"接收LLM返回信息时出现错误:{e}",file=f)
    #     f.flush()
    #     request.state=LLMRequestStateEnum.ERROR
    #     request.result = str(e)
    #     tos_queue.put(request)
