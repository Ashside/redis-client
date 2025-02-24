import shlex
import socket


def build_resp_command(command, *args):
    """
    构建 RESP 协议格式的命令
    :param command: Redis命令 (如 HSET、HDEL)
    :param args: 命令参数 (包含key和字段值)
    :return: 符合 RESP 协议的字节流
    """
    parts = [command.upper()] + list(args)
    protocol = f"*{len(parts)}\r\n"

    for part in parts:
        str_part = str(part)
        protocol += f"${len(str_part)}\r\n{str_part}\r\n"

    return protocol.encode('utf-8')
def _parse(buffer, idx):
    if idx >= len(buffer):
        return None, idx

    prefix = buffer[idx]
    idx += 1

    if prefix == b'+'[0]:  # 简单字符串
        end = buffer.index(b'\r\n', idx)
        content = buffer[idx:end].decode('utf-8')
        return content, end + 2

    elif prefix == b'-'[0]:  # 错误
        end = buffer.index(b'\r\n', idx)
        message = buffer[idx:end].decode('utf-8')
        raise RuntimeError(f"Redis错误: {message}")

    elif prefix == b':'[0]:  # 整数
        end = buffer.index(b'\r\n', idx)
        num = int(buffer[idx:end])
        return num, end + 2

    elif prefix == b'$'[0]:  # 批量字符串
        end = buffer.index(b'\r\n', idx)
        length = int(buffer[idx:end])
        idx = end + 2
        if length == -1:  # nil 响应
            return None, idx
        value = buffer[idx:idx + length]
        return value.decode('utf-8'), idx + length + 2  # 跳过结尾\r\n

    elif prefix == b'*'[0]:  # 数组
        end = buffer.index(b'\r\n', idx)
        count = int(buffer[idx:end])
        idx = end + 2
        elements = []
        for _ in range(count):
            element, idx = _parse(buffer, idx)
            elements.append(element)
        return elements, idx

    else:
        raise ValueError(f"未知协议前缀: {chr(prefix)} (ASCII: {prefix})")
def parse_redis_response(response_buffer):
    """
    完全重写的 RESP 协议解析器，支持多级嵌套和分批读取
    """



    try:
        # 将字符串转换为字节缓冲区处理
        if isinstance(response_buffer, str):
            buffer = response_buffer.encode('utf-8')
        else:
            buffer = response_buffer
        result, _ = _parse(buffer, 0)
        return result
    except Exception as e:
        raise ValueError(f"解析失败: {str(e)} 原始数据: {buffer}")


def send_redis_command(command, *args, host='127.0.0.1', port=6379, timeout=5):
    """改进后的通信函数，支持完整读取响应"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(timeout)
            sock.connect((host, port))

            # 发送命令
            sock.sendall(build_resp_command(command, *args))

            # 完整读取所有响应数据
            buffer = b''
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                buffer += chunk
                # 检查是否已读取完整响应
                try:
                    # 尝试提前解析以确认完整性
                    result, idx = _parse(buffer, 0)
                    if idx == len(buffer):
                        break
                except:
                    continue

            return parse_redis_response(buffer)

    except socket.timeout:
        raise RuntimeError("连接超时") from None
    except ConnectionRefusedError:
        raise RuntimeError("连接被拒绝") from None

def execute_redis_command(cmd_str):
    """执行自然语言命令的完整流程"""
    try:
        # 使用 shell 语法解析命令（保留引号内内容）
        parts = shlex.split(cmd_str.strip())
        if not parts:
            return "空命令"

        # 分离命令和参数
        command = parts[0].upper()
        args = parts[1:]

        # 执行命令并获取响应
        response = send_redis_command(command, *args)
        return format_output(command, response)

    except Exception as e:
        return f"执行错误: {str(e)}"


def format_output(command, response):
    """格式化输出结果"""
    cmd_type = command.upper()

    # 根据命令类型定制输出格式
    if command == "HGETALL":
        if not response:
            return "(empty list or set)"
        return "\n".join([f"{i + 1}) {k}\n   {v}"
                          for i, (k, v) in enumerate(zip(response[::2], response[1::2]))])

    if isinstance(response, list):
        return "\n".join([str(x) for x in response])

    if response is None:
        return "(nil)"

    return str(response)


def command_line_interface():
    """交互式命令行界面"""
    print("Redis 命令行客户端 (输入 EXIT 退出)")
    while True:
        try:
            # 读取输入并去除两侧空白
            user_input = input("\nredis> ").strip()
            if user_input.lower() in ('exit', 'quit'):
                break
            if not user_input:
                continue

            # 执行并显示结果
            print(execute_redis_command(user_input))

        except KeyboardInterrupt:
            print("\n使用 EXIT 命令退出")
        except EOFError:
            break

if __name__ == "__main__":
    # 启动交互界面
    command_line_interface()