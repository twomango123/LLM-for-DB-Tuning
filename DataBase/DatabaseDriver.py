from abc import ABC, abstractmethod
import time
import threading
from typing import List, Dict, Any, Tuple
from queue import Queue

class DatabaseDriver(ABC):
    """dbdriver基类 - 专注于性能评估"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        self.is_connected = False
        self._lock = threading.Lock()
    
    @abstractmethod
    def connect(self) -> bool:
        """连接数据库"""
        pass
    
    @abstractmethod
    def disconnect(self) -> bool:
        """断开数据库连接"""
        pass
    
    @abstractmethod
    def execute_query(self, query: str) -> List[Dict]:
        """执行查询"""
        pass

    @abstractmethod
    def _create_thread_connection(self):
        """为每个线程创建独立的数据库连接"""
        pass

    def execute_with_timing(self, query: str) -> Tuple[List[Dict], float]:
        """执行查询并记录时间"""
        start_time = time.time()
        try:
            result = self.execute_query(query)
            execution_time = time.time() - start_time
            return result, execution_time
        except Exception as e:
            execution_time = time.time() - start_time
            raise e

    def _execute_concurrent_query(self, query: str, duration: float, results_queue: Queue):
        """并发执行查询的线程函数"""
        query_count = 0
        thread_times = []
        end_time = time.time() + duration
        
        thread_conn = self._create_thread_connection()
        
        while time.time() < end_time:
            start_time = time.time()
            try:
                if thread_conn:
                    cursor = thread_conn.cursor()
                    cursor.execute(query)
                    cursor.fetchall()
                    cursor.close()
                execution_time = time.time() - start_time
                query_count += 1
                thread_times.append(execution_time)
            except Exception:
                # 简单记录失败，继续执行
                continue
        
        if thread_conn:
            thread_conn.close()
        
        results_queue.put({
            'query_count': query_count,
            'thread_times': thread_times
        })

    def _calculate_throughput_metrics(self, total_queries: int, total_duration: float, 
                                    all_times: List[float]) -> Dict[str, Any]:
        """计算吞吐量相关指标"""
        # 基础TPS计算
        tps = total_queries / total_duration if total_duration > 0 else 0
        qps = tps  # 对于查询来说，TPS和QPS相同
        
        # 延迟统计
        avg_latency = sum(all_times) / len(all_times) if all_times else 0
        min_latency = min(all_times) if all_times else 0
        max_latency = max(all_times) if all_times else 0
        
        # 吞吐量效率指标
        throughput_efficiency = (tps / avg_latency) if avg_latency > 0 else 0
        
        return {
            'throughput_tps': tps,
            'throughput_qps': qps,
            'total_queries': total_queries,
            'total_duration': total_duration,
            'avg_latency': avg_latency,
            'min_latency': min_latency,
            'max_latency': max_latency,
            'throughput_efficiency': throughput_efficiency,
            'queries_per_second_per_thread': tps / len(all_times) if all_times else 0
        }

    def _run_concurrent_test(self, query: str, concurrency: int, duration: float) -> Dict[str, Any]:
        """运行并发测试 - 主要测试吞吐量"""
        print(f"并发吞吐量测试 - 线程数: {concurrency}, 持续时间: {duration}秒")
        
        results_queue = Queue()
        threads = []
        all_times = []
        total_queries = 0
        
        # 记录测试开始时间
        start_time = time.time()
        
        # 启动所有并发线程
        for i in range(concurrency):
            thread = threading.Thread(
                target=self._execute_concurrent_query,
                args=(query, duration, results_queue)
            )
            threads.append(thread)
            thread.start()
        
        # 等待所有线程完成
        for thread in threads:
            thread.join()
        
        # 计算总测试时间
        total_duration = time.time() - start_time
        
        # 收集所有线程的结果
        while not results_queue.empty():
            result = results_queue.get()
            total_queries += result['query_count']
            all_times.extend(result['thread_times'])
        
        # 计算吞吐量指标
        throughput_metrics = self._calculate_throughput_metrics(total_queries, total_duration, all_times)
        
        # 构建完整结果
        results = {
            'test_mode': 'concurrent_throughput',
            'concurrency_level': concurrency,
            'target_duration': duration,
            'actual_duration': total_duration,
        }
        results.update(throughput_metrics)
        
        print(f"并发吞吐量测试完成 - TPS: {throughput_metrics['throughput_tps']:.2f}, "
              f"总查询数: {total_queries}")
        
        return results

    def _run_sequential_test(self, query: str, iterations: int) -> Dict[str, Any]:
        """运行串行测试 - 主要测试延迟"""
        print(f"串行延迟测试 - 迭代次数: {iterations}")
        
        latencies = []
        successful_runs = 0
        
        # 记录开始时间
        start_time = time.time()
        
        for i in range(iterations):
            try:
                _, execution_time = self.execute_with_timing(query)
                latencies.append(execution_time)
                successful_runs += 1
                print(f"执行 {i+1}/{iterations}: {execution_time:.4f}s")
            except Exception as e:
                print(f"执行 {i+1}/{iterations}: 失败 - {e}")
        
        # 计算总时间
        total_time = time.time() - start_time
        
        if successful_runs == 0:
            raise RuntimeError("所有SQL执行都失败了")
        
        # 计算串行模式的吞吐量（虽然主要是测延迟，但也提供吞吐量数据）
        tps = successful_runs / total_time if total_time > 0 else 0
        avg_latency = sum(latencies) / len(latencies)
        
        results = {
            'test_mode': 'sequential_latency',
            'total_iterations': iterations,
            'successful_iterations': successful_runs,
            'total_time': total_time,
            'throughput_tps': tps,
            'throughput_qps': tps,
            'avg_latency': avg_latency,
            'min_latency': min(latencies),
            'max_latency': max(latencies),
            'latencies': latencies,
            'throughput_efficiency': (tps / avg_latency) if avg_latency > 0 else 0
        }
        
        print(f"串行测试完成 - 平均延迟: {avg_latency:.4f}s, TPS: {tps:.2f}")
        
        return results

    def _warmup_sql(self, sql: str, warmup_runs: int = 3):
        """预热执行SQL"""
        print("开始预热...")
        for i in range(warmup_runs):
            try:
                start_time = time.time()
                self.execute_query(sql)
                execution_time = time.time() - start_time
                print(f"预热执行 {i+1}/{warmup_runs}: {execution_time:.4f}s")
            except Exception as e:
                print(f"预热执行 {i+1}/{warmup_runs}: 失败 - {e}")

    def evaluation(self, data: str, physical_schema: str, benchmark_sql: str, 
                  iterations: int = 10, concurrency: int = 1, 
                  duration: float = None) -> Dict[str, Any]:
        """
        L0评估函数：执行基准SQL并返回TPS和延迟
        
        Args:
            data: 数据描述
            physical_schema: 物理schema信息
            benchmark_sql: 基准测试SQL
            iterations: 执行迭代次数(串行模式)
            concurrency: 并发线程数
            duration: 并发测试持续时间(秒)
            
        Returns:
            包含TPS、延迟等指标的字典
        """
        print(f"开始性能评估")
        print(f"数据: {data}")
        print(f"物理Schema: {physical_schema}")
        print(f"测试SQL: {benchmark_sql}")
        
        # 预热执行
        self._warmup_sql(benchmark_sql)
        
        if concurrency > 1 and duration:
            # 并发吞吐量测试模式
            results = self._run_concurrent_test(benchmark_sql, concurrency, duration)
        else:
            # 串行延迟测试模式
            results = self._run_sequential_test(benchmark_sql, iterations)
        
        # 添加公共信息
        results.update({
            'data': data,
            'physical_schema': physical_schema,
            'benchmark_sql': benchmark_sql,
        })
        
        # 输出主要结果
        tps_value = results.get('throughput_tps', 0)
        latency_value = results.get('avg_latency', 0)
        print(f"评估完成 - TPS: {tps_value:.2f}, 平均延迟: {latency_value:.4f}s")
        
        return results

    # def measure_throughput_only(self, query: str, concurrency: int = 10, 
    #                            duration: float = 30.0) -> Dict[str, Any]:
    #     """
    #     专门测量吞吐量的方法
        
    #     Args:
    #         query: 测试SQL
    #         concurrency: 并发线程数
    #         duration: 测试持续时间
            
    #     Returns:
    #         吞吐量测试结果
    #     """
    #     print(f"专门吞吐量测试 - {concurrency}线程, {duration}秒")
    #     return self._run_concurrent_test(query, concurrency, duration)
