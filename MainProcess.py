import socket
import pickle
import multiprocessing
import select


class Config:
	# 服务端的ip和端口
	SOCKET_IP = ('127.0.0.1', 6666)
	# 服务端的缓冲区大小，单位为字节
	BUFFER_SIZE = 1024 * 512


class Data:
	SET = 0
	GET = 1
	DEL = 2
	QUEUE_GET = 3
	QUEUE_PUT = 4
	QUEUE_SIZE = 5
	
	def __init__(self, function, key, value=None):
		self.function = function
		self.key = key
		self.value = value


class Server:
	def __init__(self):
		self.memory = dict()
		self.queue = dict()
	
	def start_server_socket(self):
		"""
		启动服务端UDP Socket
		:return:
		"""
		ip, port = Config.SOCKET_IP
		server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # 使用UDP方式传输
		server.bind((ip, port))  # 绑定IP与端口
		server.listen(200)
		# 将 server_socket 设置为非阻塞模式
		server.setblocking(False)
		# 存储已连接的 client_socket
		connected_clients = []
		# 不断循环，接受客户端发来的消息
		while True:
			# 使用 select 函数监听所有连接的 client_socket
			readable_sockets, _, _ = select.select([server] + connected_clients, [], [])
			# 处理所有可读的 socket
			for sock in readable_sockets:
				# 如果是 server_socket 表示有新的连接
				if sock is server:
					client_socket, client_address = server.accept()
					connected_clients.append(client_socket)
				# 否则是已连接的 client_socket，需要处理收到的数据
				else:
					try:
						socket_data = sock.recv(Config.BUFFER_SIZE)
						if socket_data:
							result = self.__process_data(socket_data)
							sock.send(result)
						else:
							sock.close()
							connected_clients.remove(sock)
					except Exception:
						# 出现异常，也需要关闭 socket 并从 connected_clients 中移除
						sock.close()
						connected_clients.remove(sock)
	
	def __process_data(self, data):
		data = pickle.loads(data)
		result = None
		if data.function == Data.GET:
			result = self.__get_data(data.key)
		elif data.function == Data.SET:
			result = self.__set_data(data.key, data.value)
		elif data.function == Data.DEL:
			result = self.__del_data(data.key)
		elif data.function == Data.QUEUE_GET:
			result = self.__queue_get(data.key)
		elif data.function == Data.QUEUE_PUT:
			self.__queue_put(data.key, data.value)
		elif data.function == Data.QUEUE_SIZE:
			result = self.__queue_size(data.key)
		result = pickle.dumps(result)
		return result
	
	def __get_data(self, key):
		result = None
		if key in self.memory.keys():
			result = self.memory[key]
		return result
	
	def __set_data(self, key, value):
		result = None
		if key in self.memory.keys():
			result = self.memory[key]
		self.memory[key] = value
		return result
	
	def __del_data(self, key):
		result = None
		if key in self.memory.keys():
			result = self.memory.pop(key)
		return result
	
	def __queue_get(self, key):
		result = None
		if key in self.queue.keys() and len(self.queue[key]) > 0:
			result = self.queue[key].pop(0)
		return result
	
	def __queue_put(self, key, value):
		if key not in self.queue.keys():
			self.queue[key] = list()
		self.queue[key].append(value)
	
	def __queue_size(self, key):
		result = 0
		if key in self.queue.keys():
			result = len(self.queue[key])
		return result


class Client:
	def __init__(self, ip, port):
		client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # 使用TCP方式传输
		client.connect((ip, port))  # 连接远程服务端
		self.client = client
	
	def get_data(self, key):
		data = Data(Data.GET, key)
		result = self.__request(data)
		return result
	
	def set_data(self, key, value):
		data = Data(Data.SET, key, value)
		result = self.__request(data)
		return result
	
	def del_data(self, key):
		data = Data(Data.DEL, key)
		result = self.__request(data)
		return result
	
	def queue_get(self, key):
		data = Data(Data.QUEUE_GET, key)
		result = self.__request(data)
		return result
	
	def queue_put(self, key, value):
		data = Data(Data.QUEUE_PUT, key, value)
		result = self.__request(data)
		return result
	
	def queue_size(self, key):
		data = Data(Data.QUEUE_SIZE, key)
		result = self.__request(data)
		return result
	
	def __request(self, data):
		data = pickle.dumps(data)
		self.client.sendto(data, Config.SOCKET_IP)  # 使用sendto发送UDP消息，address填入服务端IP和端口
		result = self.client.recv(Config.BUFFER_SIZE)
		return pickle.loads(result)
	
	def close(self):
		self.client.close()


def create_shared_memory():
	"""
	创建共享内存的客户端，先判断服务端是否已经创建，如果没有就创建服务端。最后返回客户端
	:return:
	"""
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	result = sock.connect_ex(Config.SOCKET_IP)
	if result != 0:
		server = Server()
		process = multiprocessing.Process(target=server.start_server_socket, args=())
		process.start()
		while True:
			result = sock.connect_ex(Config.SOCKET_IP)
			if result == 0:
				break
	ip, port = Config.SOCKET_IP
	client = Client(ip, port)
	return client


if __name__ == '__main__':
	shared_memory1 = create_shared_memory()
	shared_memory2 = create_shared_memory()
