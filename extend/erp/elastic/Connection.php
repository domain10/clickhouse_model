<?php
/**
 * Copyright: Foudaful
 * Author: sky <sky.nie@qq.com>
 * Date: 2019/10/08 11:00
 **/

namespace erp\elastic;

use Elasticsearch\ClientBuilder;
use think\Db;
use think\Debug;
use think\Exception;
use think\Log;

/**
 * Mongo数据库驱动
 */
class Connection
{
    protected $dbName = ''; // dbName
    /** @var string 当前SQL指令 */
    protected $queryStr = '';
    // 查询数据类型
    protected $typeMap = 'array';

    // 监听回调
    protected static $event = [];
    /** @var [] 数据库连接ID 支持多个连接 */
    protected $links = [];
    /** @var 当前连接ID */
    protected $linkID;
    protected $linkRead;
    protected $linkWrite;

    // 返回或者影响记录数
    protected $numRows = 0;
    // 最后插入ID
    protected $insertId = [];
    // 错误信息
    protected $error = '';
    // 查询对象
    protected $query = [];
    // 查询参数
    protected $options = [];
    // 数据库连接参数配置
    protected $config = [
        // 数据库类型
        'type'          => '',
        // es服务器连接地址
        'hostname'        => '',
        // 端口
        'hostport'        => '',
        // 数据库名
        'database'        => '',
        // 用户名
        'username'        => '',
        // 密码
        'password'        => '',
        // 连接dsn
        'dsn'             => '',
        // 数据库连接参数
        'params'          => [],
        // 数据库编码默认采用utf8
        'charset'         => 'utf8',
        // 数据库表前缀
        'prefix'          => '',
        'default_limit'   => 1000,
        'max_page_offset' => 100000,
        'pk'              => 'id',
        // 数据库调试模式
        'debug'           => false,
        // 数据库部署方式:0 集中式(单一服务器),1 分布式(主从服务器)
        'deploy'          => 0,
        // 数据库读写是否分离 主从式有效
        'rw_separate'     => false,
        // 读写分离后 主服务器数量
        'master_num'      => 1,
        // 指定从服务器序号
        'slave_no'        => '',
        // 是否严格检查字段是否存在
        'fields_strict'   => true,
        // 数据集返回类型
        'resultset_type'  => 'array',
        // 自动写入时间戳字段
        'auto_timestamp'  => false,
        // 时间字段取出后的默认时间格式
        'datetime_format' => 'Y-m-d H:i:s',
        // 是否需要进行SQL性能分析
        'sql_explain'     => false,
        // Query对象
        'query'           => '\erp\elastic\Query',
        'es_type'         => '_doc'
    ];

    /**
     * 架构函数 读取数据库配置信息
     * @access public
     * @param array $config 数据库配置数组
     */
    public function __construct(array $config = [])
    {
        if (!empty($config)) {
            $this->config = array_merge($this->config, $config);
        }
    }

    /**
     * 连接数据库方法
     * @access public
     * @param array         $config 连接参数
     * @param integer       $linkNum 连接序号
     */
    public function connect(array $config = [], $linkNum = 0)
    {
        if (!isset($this->links[$linkNum])) {
            if (empty($config)) {
                $config = $this->config;
            } else {
                $config = array_merge($this->config, $config);
            }
            if ($config['debug']) {
                $startTime = microtime(true);
            }
            $this->links[$linkNum] = ClientBuilder::create()->setHosts($config['hosts'])->build();
            if ($config['debug']) {
                // 记录数据库连接信息
                Log::record('[ Elastic ] CONNECT:[ UseTime:' . number_format(microtime(true) - $startTime, 6) . 's ] ' . json_encode($config['hosts']), 'es');
            }
        }
        return $this->links[$linkNum];
    }

    /**
     * 指定当前使用的查询对象
     * @access public
     * @param Query $query 查询对象
     * @return $this
     */
    public function setQuery($query, $model = 'db')
    {
        $this->query[$model] = $query;
        return $this;
    }

    /**
     * 创建指定模型的查询对象
     * @access public
     * @param string $model 模型类名称
     * @param string $queryClass 查询对象类名
     * @return Query
     */
    public function getQuery($model = 'db', $queryClass = '')
    {
        if (!isset($this->query[$model])) {
            $class               = $queryClass ?: $this->config['query'];
            $this->query[$model] = new $class($this, 'db' == $model ? '' : $model);
        }
        return $this->query[$model];
    }

    /**
     * 调用Query类的查询方法
     * @access public
     * @param string    $method 方法名称
     * @param array     $args 调用参数
     * @return mixed
     */
    public function __call($method, $args)
    {
        return call_user_func_array([$this->getQuery(), $method], $args);
    }

    /**
     * 获取数据库的配置参数
     * @access public
     * @param string $config 配置名称
     * @return mixed
     */
    public function getConfig($config = '')
    {
        return $config ? ($this->config[$config] ?? null) : $this->config;
    }

    /**
     * 设置数据库的配置参数
     * @access public
     * @param string    $config 配置名称
     * @param mixed     $value 配置值
     * @return void
     */
    public function setConfig($config, $value)
    {
        $this->config[$config] = $value;
    }

    /**
     * 获取连接对象
     * @access public
     * @return object|null
     */
    public function getLink()
    {
        if (!$this->linkID) {
            return;
        } else {
            return $this->linkID;
        }
    }

    /**
     * 设置/获取当前操作的database
     * @access public
     * @param string  $db db
     * @throws Exception
     */
    public function db($db = null)
    {
        if (is_null($db)) {
            return $this->dbName;
        } else {
            $this->dbName = $db;
        }
    }
    
    /**
     * 获取最后写入的ID 如果是insertAll方法的话 返回所有写入的ID
     * @access public
     * @return mixed
     */
    public function getLastInsID()
    {
        return $this->insertId;
    }
    
    /**
     * ObjectID处理
     * @access public
     * @param array     $data
     * @return void
     */
    private function convertObjectID(&$data)
    {
        if (isset($data['_id'])) {
            $data['id'] = $data['_id']->__toString();
            unset($data['_id']);
        }
    }
    
    public function indices(array $params)
    {
        $this->initConnect(false);
        return $this->unify('indices', $params);
    }

    /**
     * 执行查询
     * @access public
     * @param array             $query 查询参数
     * @param string|bool       $class 返回的数据集类型
     * @param string|null       $op 操作类型
     * @return mixed
     */
    public function query(array $params, $class = false, $op = null)
    {
        $this->initConnect(false);
        $result = $this->unify('search', $params);
        //
        if('agg' == $op) {
            return $result['aggregations']['agg_result'] ?? [];
        } else {
            $result = $result['hits']['hits'] ?? [];
            return array_map(function($v){return $v['_source'];}, $result);
            //return $this->getResult($class, $typeMap);
        }
    }
    
    /**
     * 执行count
     * @access public
     * @param array        $query 查询参数
     * @return int
     */
    public function count(array $params)
    {
        $this->initConnect(false);
        $result = $this->unify('count', $params);
        
        return $result['count'] ?? 0;
    }

    /**
     * 执行写操作
     * @access public
     * @param string    $index
     * @param array     $bulk
     * @return WriteResult
     */
    public function execute(array $bulk)
    {
        $this->initConnect(true);
        $result = $this->unify('bulk', $bulk);
        
        if (! empty($result['errors'])) {
            $err = array_map(function ($v) {$v = current($v); return $v['error'] ?? '';}, $result['items']);
            throw new Exception(json_encode($err));
        }
        $this->numRows = count($result['items']);
        $this->insertId = array_map(function ($v) {$v = current($v); return $v['_id'];}, $result['items']);
        $result = null;
        return $this->numRows;
    }
    
    public function updateByQuery(array $params)
    {
        $this->initConnect(true);
        $result = $this->unify('updateByQuery', $params);
        
        $err = empty($result['failures']) ? '' : $result['failures'];
        if ($err) {
            throw new Exception(json_encode($err));
        }
        $this->numRows = $result['updated'] ?? 0;
        $result = null;
        return $this->numRows;
    }
    
    public function deleteByQuery(array $params)
    {
        $this->initConnect(true);
        $result = $this->unify('updateByQuery', $params);
        
        $err = empty($result['failures']) ? '' : $result['failures'];
        if ($err) {
            throw new \Exception(json_encode($err));
        }
        $this->numRows = $result['deleted'] ?? 0;
        $result = null;
        return $this->numRows;
    }

    /**
     * 数据库日志记录（仅供参考）
     * @access public
     * @param string $type 类型
     * @param mixed  $data 数据
     * @param array  $options 参数
     * @return void
     */
    public function log($type, $data, $options = [])
    {
        if (!$this->config['debug']) {
            return;
        }
        switch (strtolower($type)) {
            case 'find':
            case 'aggregate':
            case 'insert':
            case 'update':
//                 $this->queryStr = $type . '(' . json_encode($options) . ',' . json_encode($data) . ');';
//                 break;
            case 'remove':
                $this->queryStr = $type . '(' . ($data ? json_encode($data) : '') . ');';
                break;
            case 'cmd':
                $this->queryStr = $data . '(' . json_encode($options) . ');';
                break;
        }
        $this->options = $options;
    }

    /**
     * 获取执行的指令
     * @access public
     * @return string
     */
    public function getQueryStr()
    {
        return $this->queryStr;
    }

    /**
     * 监听SQL执行
     * @access public
     * @param callable $callback 回调方法
     * @return void
     */
    public function listen($callback)
    {
        self::$event[] = $callback;
    }

    /**
     * 触发SQL事件
     * @access protected
     * @param string    $sql 语句
     * @param float     $runtime 运行时间
     * @param array     $options 参数
     * @return bool
     */
    protected function trigger($sql, $runtime, $options = [])
    {
        if (!empty(self::$event)) {
            foreach (self::$event as $callback) {
                if (is_callable($callback)) {
                    call_user_func_array($callback, [$sql, $runtime, $options]);
                }
            }
        } else {
            // 未注册监听则记录到日志中
            Log::record('[ Elastic ] ' . $sql . ' [ RunTime:' . $runtime . 's ]', 'es_log');
        }
    }

    /**
     * 数据库调试 记录当前SQL及分析性能
     * @access protected
     * @param boolean $start 调试开始标记 true 开始 false 结束
     * @param string  $sql 执行的SQL语句 留空自动获取
     * @return void
     */
    protected function debug($start, $sql = '')
    {
        if (!empty($this->config['debug'])) {
            // 开启数据库调试模式
            if ($start) {
                Debug::remark('queryStartTime', 'time');
            } else {
                // 记录操作结束时间
                Debug::remark('queryEndTime', 'time');
                $runtime = Debug::getRangeTime('queryStartTime', 'queryEndTime');
                $sql     = $sql ?: $this->queryStr;
                // SQL监听
                $this->trigger($sql, $runtime, $this->options);
            }
        }
    }

    /**
     * 释放查询结果
     * @access public
     */
    public function free()
    {
        $this->cursor = null;
    }

    /**
     * 关闭数据库
     * @access public
     */
    public function close()
    {
        $this->linkID     = null;
        $this->cursor    = null;
        $this->linkRead  = null;
        $this->linkWrite = null;
        $this->links     = [];
    }
    
    /**
     * 统一操作
     * @access protected
     */
    protected function unify(string $func, array $params)
    {
        try {
            $this->debug(true);
            switch ($func) {
                case 'indices':
                    $result = $this->linkID->indices()->getMapping($params);
                    break;
                default:
                    $result = $this->linkID->$func($params);
                    break;
            }
            $this->debug(false);
        } catch (\Throwable $e) {
           throw new \Exception($e->getMessage());
        }
        return $result;
    }

    /**
     * 初始化数据库连接
     * @access protected
     * @param boolean $master 是否主服务器
     * @return void
     */
    protected function initConnect($master = true)
    {
        if (!empty($this->config['deploy'])) {
            // 采用分布式
            if ($master) {
                if (!$this->linkWrite) {
                    $this->linkWrite = $this->multiConnect(true);
                }
                $this->linkID = $this->linkWrite;
            } else {
                if (!$this->linkRead) {
                    $this->linkRead = $this->multiConnect(false);
                }
                $this->linkID = $this->linkRead;
            }
        } elseif (!$this->linkID) {
            // 默认单数据库
            $this->linkID = $this->connect();
        }
    }

    /**
     * 连接分布式服务器
     * @access protected
     * @param boolean $master 主服务器
     * @return PDO
     */
    protected function multiConnect($master = false)
    {
        $_config = [];
        // 分布式数据库配置解析
        foreach (['username', 'password', 'hostname', 'hostport', 'database', 'dsn', 'charset'] as $name) {
            $_config[$name] = explode(',', $this->config[$name]);
        }

        // 主服务器序号
        $m = floor(mt_rand(0, $this->config['master_num'] - 1));

        if ($this->config['rw_separate']) {
            // 主从式采用读写分离
            if ($master) // 主服务器写入
            {
                if ($this->config['is_replica_set']) {
                    return $this->replicaSetConnect();
                } else {
                    $r = $m;
                }
            } elseif (is_numeric($this->config['slave_no'])) {
                // 指定服务器读
                $r = $this->config['slave_no'];
            } else {
                // 读操作连接从服务器 每次随机连接的数据库
                $r = floor(mt_rand($this->config['master_num'], count($_config['hostname']) - 1));
            }
        } else {
            // 读写操作不区分服务器 每次随机连接的数据库
            $r = floor(mt_rand(0, count($_config['hostname']) - 1));
        }
        $dbConfig = [];
        foreach (['username', 'password', 'hostname', 'hostport', 'database', 'dsn', 'charset'] as $name) {
            $dbConfig[$name] = isset($_config[$name][$r]) ? $_config[$name][$r] : $_config[$name][0];
        }
        return $this->connect($dbConfig, $r);
    }

    /**
     * 析构方法
     * @access public
     */
    public function __destruct()
    {
        // 释放查询
        $this->free();

        // 关闭连接
        $this->close();
    }
}
