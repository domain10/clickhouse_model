<?php
/**
 * Copyright: Foudaful
 * Author: sky <sky.nie@qq.com>
 * Date: 2019/09/30 10:00
 **/

namespace erp\elastic;

use think\Cache;
use think\Collection;
use think\Config;
use think\Db;
use think\db\exception\DataNotFoundException;
use think\db\exception\ModelNotFoundException;
use think\Exception;
use think\exception\DbException;
use think\Loader;
use think\Model;
use think\Paginator;

class Query
{
    // 数据库Connection对象实例
    protected $connection;
    // 数据库Builder对象实例
    protected $builder;
    // 当前模型类名称
    protected $model;
    // 当前数据表名称（含前缀）
    protected $table = '';
    // 当前数据表名称（不含前缀）
    protected $name = '';
    // 当前数据表主键
    protected $pk;
    // 当前数据表前缀
    protected $prefix = '';
    // 查询参数
    protected $options = [];
    // 数据表信息
    protected static $info = [];
    // 回调事件
    private static $event = [];

    /**
     * 架构函数
     * @access public
     * @param Connection    $connection 数据库对象实例
     * @param string        $model 模型名
     */
    public function __construct(Connection $connection = null, $model = '')
    {
        $this->connection = $connection ?: Db::connect([], true);
        $this->prefix     = $this->connection->getConfig('prefix');
        $this->model      = $model;
        // 设置当前连接的Builder对象
        $this->setBuilder();
    }

    /**
     * 利用__call方法实现一些特殊的Model方法
     * @access public
     * @param string    $method 方法名称
     * @param array     $args 调用参数
     * @return mixed
     * @throws DbException
     * @throws Exception
     */
    public function __call($method, $args)
    {
        if (strtolower(substr($method, 0, 5)) == 'getby') {
            // 根据某个字段获取记录
            $field         = Loader::parseName(substr($method, 5));
            $where[$field] = $args[0];
            return $this->where($where)->find();
        } elseif (strtolower(substr($method, 0, 10)) == 'getfieldby') {
            // 根据某个字段获取记录的某个值
            $name         = Loader::parseName(substr($method, 10));
            $where[$name] = $args[0];
            return $this->where($where)->value($args[1]);
        } else {
            throw new Exception('method not exists:' . __CLASS__ . '->' . $method);
        }
    }

    /**
     * 获取当前的数据库Connection对象
     * @access public
     * @return Connection
     */
    public function getConnection()
    {
        return $this->connection;
    }

    /**
     * 切换当前的数据库连接
     * @access public
     * @param mixed $config
     * @return $this
     */
    public function connect($config)
    {
        $this->connection = Db::connect($config);
        $this->setBuilder();
        return $this;
    }

    /**
     * 设置当前的数据库Builder对象
     * @access protected
     * @return void
     */
    protected function setBuilder()
    {
        $this->builder = new Builder($this->connection, $this);
    }

    /**
     * 指定默认的数据表名（不含前缀）
     * @access public
     * @param string $name
     * @return $this
     */
    public function name($name)
    {
        $this->name = $name;
        return $this;
    }

    /**
     * 指定默认数据表名（含前缀）
     * @access public
     * @param string $table 表名
     * @return $this
     */
    public function setTable($table)
    {
        $this->table = $table;
        return $this;
    }

    /**
     * 得到当前或者指定名称的数据表
     * @access public
     * @param string $name
     * @return string
     */
    public function getTable($name = '')
    {
        if ($name || empty($this->table)) {
            $name      = $name ?: $this->name;
            $tableName = $this->prefix;
            if ($name) {
                $tableName .= Loader::parseName($name);
            }
        } else {
            $tableName = $this->table;
        }
        return $tableName;
    }

    /**
     * 指定数据表主键
     * @access public
     * @param string $pk 主键
     * @return $this
     */
    public function pk($pk)
    {
        $this->pk = $pk;
        return $this;
    }

    /**
     * 去除某个查询条件
     * @access public
     * @param string $field 查询字段
     * @param string $logic 查询逻辑 must or xor
     * @return $this
     */
    public function removeWhereField($field, $logic = 'must')
    {
        $logic = strtolower($logic);
        if (isset($this->options['where'][$logic][$field])) {
            unset($this->options['where'][$logic][$field]);
        }
        return $this;
    }

    /**
     * 去除查询参数
     * @access public
     * @param string|bool $option 参数名 true 表示去除所有参数
     * @return $this
     */
    public function removeOption($option = true)
    {
        if (true === $option) {
            $this->options = [];
        } elseif (is_string($option) && isset($this->options[$option])) {
            unset($this->options[$option]);
        }
        return $this;
    }

    /**
     * 将SQL语句中的__TABLE_NAME__字符串替换成带前缀的表名（小写）
     * @access public
     * @param string $sql sql语句
     * @return string
     */
    public function parseSqlTable($sql)
    {
        if (false !== strpos($sql, '__')) {
            $prefix = $this->prefix;
            $sql    = preg_replace_callback("/__([A-Z0-9_-]+)__/sU", function ($match) use ($prefix) {
                return $prefix . strtolower($match[1]);
            }, $sql);
        }
        return $sql;
    }

    /**
     * 执行查询 返回数据集
     * @access public
     * @param string            $index
     * @param array             $query 查询
     * @param bool|string       $class 指定返回的数据集对象
     * @param null|string       $op 操作类型
     * @return mixed
     * @throws Exception
     */
    public function query($query, $class = false, $op = null)
    {
        return $this->connection->query($query, $class, $op);
    }

    /**
     * 执行语句
     * @access public
     * @param string    $index
     * @param array     $bulk
     */
    public function execute(array $bulk)
    {
        return $this->connection->execute($bulk);
    }

    /**
     * 获取最近插入的ID
     * @access public
     * @return string
     */
    public function getLastInsID()
    {
        return $this->connection->getLastInsID();
    }

    /**
     * 获取最近一次执行的指令
     * @access public
     * @return string
     */
    public function getLastSql()
    {
        return $this->connection->getQueryStr();
    }

    /**
     * 获取数据库的配置参数
     * @access public
     * @param string $name 参数名称
     * @return boolean
     */
    public function getConfig($name = '')
    {
        return $this->connection->getConfig($name);
    }

    /**
     * 得到某个字段的值
     * @access public
     * @param string    $field 字段名
     * @param mixed     $default 默认值
     * @return mixed
     */
    public function value($field, $default = null)
    {
        $result = null;
        if (!empty($this->options['cache'])) {
            // 判断查询缓存
            $cache = $this->options['cache'];
            if (empty($this->options['table'])) {
                $this->options['table'] = $this->getTable();
            }
            $key    = is_string($cache['key']) ? $cache['key'] : md5($field . serialize($this->options));
            $result = Cache::get($key);
        }
        if (!$result) {
            if (isset($this->options['field'])) {
                unset($this->options['field']);
            }
            $result = $this->field($field)->fetchCursor(true)->find();
//             $cursor = $this->field($field)->fetchCursor(true)->find();
//             $cursor->setTypeMap(['root' => 'array']);
//             $resultSet = $cursor->toArray();
//             $data      = isset($resultSet[0]) ? $resultSet[0] : null;
//             $result    = $data[$field];
            if (isset($cache)) {
                // 缓存数据
                $this->cacheData($key, $result, $cache);
            }
        } else {
            // 清空查询条件
            $this->options = [];
        }
        return !is_null($result) ? $result : $default;
    }

    /**
     * 得到某个列的数组
     * @access public
     * @param string $field 字段名 多个字段用逗号分隔
     * @param string $key 索引
     * @return array
     */
    public function column($field, $key = '')
    {
        $result = false;
        if (!empty($this->options['cache'])) {
            // 判断查询缓存
            $cache = $this->options['cache'];
            if (empty($this->options['table'])) {
                $this->options['table'] = $this->getTable();
            }
            $guid   = is_string($cache['key']) ? $cache['key'] : md5($field . serialize($this->options));
            $result = Cache::get($guid);
        }
        if (!$result) {
            if (isset($this->options['field'])) {
                unset($this->options['field']);
            }
            if ($key && '*' != $field) {
                $field = $key . ',' . $field;
            }
            $resultSet = $this->field($field)->fetchCursor(true)->select();
//             $cursor = $this->field($field)->fetchCursor(true)->select();
//             $cursor->setTypeMap(['root' => 'array']);
//             $resultSet = $cursor->toArray();
            if ($resultSet) {
                $fields = array_keys($resultSet[0]);
                $count  = count($fields);
                $key1   = array_shift($fields);
                $key2   = $fields ? array_shift($fields) : '';
                $key    = $key ?: $key1;
                foreach ($resultSet as $val) {
                    $name = $val[$key];
                    if (2 == $count) {
                        $result[$name] = $val[$key2];
                    } elseif (1 == $count) {
                        $result[$name] = $val[$key1];
                    } else {
                        $result[$name] = $val;
                    }
                }
            } else {
                $result = [];
            }

            if (isset($cache) && isset($guid)) {
                // 缓存数据
                $this->cacheData($guid, $result, $cache);
            }
        } else {
            // 清空查询条件
            $this->options = [];
        }
        return $result;
    }

    /**
     * 指定distinct查询
     * @access public
     * @param string $field 字段名
     * @return array
     */
    public function distinct(string $field)
    {
        $result = $this->aggregate('distinct', $field);
        return $result[0]['values'];
    }

    /**
     * COUNT查询
     * @access public
     * @return integer
     */
    public function count()
    {
        // 分析查询表达式
        $options = $this->parseExpress();
        // 生成查询
        $query = $this->builder->select($options);
        $result = $this->connection->count($query);
        
        return $result;
    }

    /**
     * 多聚合操作
     *
     * @param array $aggregate 聚合指令, 可以聚合多个参数, 如 ['sum' => 'field1', 'avg' => 'field2']
     * @param array $groupBy 类似mysql里面的group字段, 可以传入多个字段, 如 ['field_a', 'field_b', 'field_c']
     * @return array 查询结果
     */
//     public function multiAggregate($aggregate, $groupBy)
//     {
//         $result = $this->cmd('multiAggregate', [$aggregate, $groupBy]);
//         $result = isset($result[0]['result']) ? $result[0]['result'] : [];
//         foreach ($result as &$row) {
//             if (isset($row['_id']) && !empty($row['_id'])) {
//                 foreach ($row['_id'] as $k => $v) {
//                     $row[$k] = $v;
//                 }
//                 unset($row['_id']);
//             }
//         }
//         return $result;
//     }
    
    /**
     * 聚合查询
     * @access public
     * @param string $aggregate 聚合指令
     * @param string $field     字段名
     * @return mixed
     */
    public function aggregate($cmd, $field)
    {
        $options = $this->parseExpress();
        $query = $this->builder->aggregate($options, ['distinct' == $cmd ? 'terms' : $cmd, $field]);
        $result = $this->query($query, $options['fetch_cursor'], 'agg');
        if ('distinct' == $cmd) {
            $result = $result['buckets'] ?? [];
            return array_map(function($arr) use($field) {return [$field => $arr['key']];}, $result);
        } else {
            return $result['value'] ?? 0;
        }
    }

    /**
     * MAX查询
     * @access public
     * @param string $field   字段名
     * @return float
     */
    public function max($field)
    {
        return $this->aggregate('max', $field);
    }

    /**
     * MIN查询
     * @access public
     * @param string $field   字段名
     * @return mixed
     */
    public function min($field)
    {
        return $this->aggregate('min', $field);
    }

    /**
     * SUM查询
     * @access public
     * @param string $field   字段名
     * @return float
     */
    public function sum($field)
    {
        return $this->aggregate('sum', $field);
    }

    /**
     * AVG查询
     * @access public
     * @param string $field   字段名
     * @return float
     */
    public function avg($field)
    {
        return $this->aggregate('avg', $field);
    }

    /**
     * 设置记录的某个字段值
     * 支持使用数据库字段和方法
     * @access public
     * @param string|array  $field 字段名
     * @param mixed         $value 字段值
     * @return integer
     */
    public function setField($field, $value = '')
    {
        if (is_array($field)) {
            $data = $field;
        } else {
            $data[$field] = $value;
        }
        return $this->update($data);
    }

    /**
     * 字段值(延迟)增长
     * @access public
     * @param string    $field 字段名
     * @param integer   $step 增长值
     * @param integer   $lazyTime 延时时间(s)
     * @return integer|true
     * @throws Exception
     */
    public function setInc($field, $step = 1, $lazyTime = 0)
    {
        $condition = !empty($this->options['where']) ? $this->options['where'] : [];
        if (empty($condition)) {
            // 没有条件不做任何更新
            throw new Exception('no data to update');
        }
        if ($lazyTime > 0) {
            // 延迟写入
            $guid = md5($this->getTable() . '_' . $field . '_' . serialize($condition));
            $step = $this->lazyWrite($guid, $step, $lazyTime);
            if (empty($step)) {
                return true; // 等待下次写入
            }
        }
        return $this->setField($field, ['$inc', $step]);
    }

    /**
     * 字段值（延迟）减少
     * @access public
     * @param string    $field 字段名
     * @param integer   $step 减少值
     * @param integer   $lazyTime 延时时间(s)
     * @return integer|true
     * @throws Exception
     */
    public function setDec($field, $step = 1, $lazyTime = 0)
    {
        $condition = !empty($this->options['where']) ? $this->options['where'] : [];
        if (empty($condition)) {
            // 没有条件不做任何更新
            throw new Exception('no data to update');
        }
        if ($lazyTime > 0) {
            // 延迟写入
            $guid = md5($this->getTable() . '_' . $field . '_' . serialize($condition));
            $step = $this->lazyWrite($guid, -$step, $lazyTime);
            if (empty($step)) {
                return true; // 等待下次写入
            }
        }
        return $this->setField($field, ['$inc', -1 * $step]);
    }

    /**
     * 延时更新检查 返回false表示需要延时
     * 否则返回实际写入的数值
     * @access public
     * @param string    $guid 写入标识
     * @param integer   $step 写入步进值
     * @param integer   $lazyTime 延时时间(s)
     * @return false|integer
     */
    protected function lazyWrite($guid, $step, $lazyTime)
    {
        if (false !== ($value = Cache::get($guid))) {
            // 存在缓存写入数据
            if ($_SERVER['REQUEST_TIME'] > Cache::get($guid . '_time') + $lazyTime) {
                // 延时更新时间到了，删除缓存数据 并实际写入数据库
                Cache::rm($guid);
                Cache::rm($guid . '_time');
                return $value + $step;
            } else {
                // 追加数据到缓存
                Cache::set($guid, $value + $step, 0);
                return false;
            }
        } else {
            // 没有缓存数据
            Cache::set($guid, $step, 0);
            // 计时开始
            Cache::set($guid . '_time', $_SERVER['REQUEST_TIME'], 0);
            return false;
        }
    }

    /**
     * 设置数据
     * @access public
     * @param mixed $field 字段名或者数据
     * @param mixed $value 字段值
     * @return $this
     */
    public function data($field, $value = null)
    {
        if (is_array($field)) {
            $this->options['data'] = isset($this->options['data']) ? array_merge($this->options['data'], $field) : $field;
        } else {
            $this->options['data'][$field] = $value;
        }
        return $this;
    }

    /**
     * 字段值增长
     * @access public
     * @param string|array $field 字段名
     * @param integer      $step  增长值
     * @return $this
     */
    public function inc($field, $step = 1)
    {
        $fields = is_string($field) ? explode(',', $field) : $field;
        foreach ($fields as $field) {
            $this->data($field, ['$inc', $step]);
        }
        return $this;
    }

    /**
     * 字段值减少
     * @access public
     * @param string|array $field 字段名
     * @param integer      $step  减少值
     * @return $this
     */
    public function dec($field, $step = 1)
    {
        $fields = is_string($field) ? explode(',', $field) : $field;
        foreach ($fields as $field) {
            $this->data($field, ['$inc', -1 * $step]);
        }
        return $this;
    }

    /**
     * 指定AND查询条件
     * @access public
     * @param mixed $field 查询字段
     * @param mixed $op 查询表达式
     * @param mixed $condition 查询条件
     * @return $this
     */
    public function where($field, $op = null, $condition = null)
    {
        $param = func_get_args();
        array_shift($param);
        $this->parseWhereExp('must', $field, $op, $condition, $param);
        return $this;
    }

    /**
     * 指定OR查询条件
     * @access public
     * @param mixed $field 查询字段
     * @param mixed $op 查询表达式
     * @param mixed $condition 查询条件
     * @return $this
     */
    public function whereOr($field, $op = null, $condition = null)
    {
        $param = func_get_args();
        array_shift($param);
        $this->parseWhereExp('should', $field, $op, $condition, $param);
        return $this;
    }

    /**
     * 指定NOR查询条件
     * @access public
     * @param mixed $field 查询字段
     * @param mixed $op 查询表达式
     * @param mixed $condition 查询条件
     * @return $this
     */
//     public function whereNor($field, $op = null, $condition = null)
//     {
//         $param = func_get_args();
//         array_shift($param);
//         $this->parseWhereExp('nor', $field, $op, $condition, $param);
//         return $this;
//     }

    /**
     * 指定Null查询条件
     * @access public
     * @param mixed  $field 查询字段
     * @param string $logic 查询逻辑 and or
     * @return $this
     */
    public function whereNull($field, $logic = 'AND')
    {
        $logic = strtolower($logic) == 'and' ? 'must' : 'should';
        $this->parseWhereExp($logic, $field, 'null', null);
        return $this;
    }

    /**
     * 指定NotNull查询条件
     * @access public
     * @param mixed  $field 查询字段
     * @param string $logic 查询逻辑 and or xor
     * @return $this
     */
    public function whereNotNull($field, $logic = 'AND')
    {
        $logic = strtolower($logic) == 'and' ? 'must' : 'should';
        $this->parseWhereExp($logic, $field, 'notnull', null);
        return $this;
    }

    /**
     * 指定In查询条件
     * @access public
     * @param mixed  $field     查询字段
     * @param mixed  $condition 查询条件
     * @param string $logic     查询逻辑 and or
     * @return $this
     */
    public function whereIn($field, $condition, $logic = 'AND')
    {
        $logic = strtolower($logic) == 'and' ? 'must' : 'should';
        $this->parseWhereExp($logic, $field, 'in', $condition);
        return $this;
    }

    /**
     * 指定NotIn查询条件
     * @access public
     * @param mixed  $field     查询字段
     * @param mixed  $condition 查询条件
     * @param string $logic     查询逻辑 and or
     * @return $this
     */
    public function whereNotIn($field, $condition, $logic = 'AND')
    {
        $logic = strtolower($logic) == 'and' ? 'must' : 'should';
        $this->parseWhereExp($logic, $field, 'not in', $condition);
        return $this;
    }

    /**
     * 指定Like查询条件
     * @access public
     * @param mixed  $field     查询字段
     * @param mixed  $condition 查询条件
     * @param string $logic     查询逻辑 and or
     * @return $this
     */
    public function whereLike($field, $condition, $logic = 'AND')
    {
        $logic = strtolower($logic) == 'and' ? 'must' : 'should';
        $this->parseWhereExp($logic, $field, 'like', $condition);
        return $this;
    }

    /**
     * 指定Between查询条件
     * @access public
     * @param mixed  $field     查询字段
     * @param mixed  $condition 查询条件
     * @param string $logic     查询逻辑 and or
     * @return $this
     */
    public function whereBetween($field, $condition, $logic = 'AND')
    {
        $logic = strtolower($logic) == 'and' ? 'must' : 'should';
        $this->parseWhereExp($logic, $field, 'between', $condition);
        return $this;
    }

    /**
     * 指定NotBetween查询条件
     * @access public
     * @param mixed  $field     查询字段
     * @param mixed  $condition 查询条件
     * @param string $logic     查询逻辑 and or
     * @return $this
     */
    public function whereNotBetween($field, $condition, $logic = 'AND')
    {
        $logic = strtolower($logic) == 'and' ? 'must' : 'should';
        $this->parseWhereExp($logic, $field, 'not between', $condition);
        return $this;
    }

    /**
     * 指定Exp查询条件
     * @access public
     * @param mixed  $field     查询字段
     * @param mixed  $condition 查询条件
     * @param string $logic     查询逻辑 must or
     * @return $this
     */
//     public function whereExp($field, $condition, $logic = 'must')
//     {
//         $this->parseWhereExp($logic, $field, 'exp', $condition);
//         return $this;
//     }

    /**
     * 分析查询表达式
     * @access public
     * @param string                $logic 查询逻辑    must should
     * @param string|array|\Closure $field 查询字段
     * @param mixed                 $op 查询表达式
     * @param mixed                 $condition 查询条件
     * @param array                 $param 查询参数
     * @return void
     */
    protected function parseWhereExp($logic, $field, $op, $condition, $param = [])
    {
        $logic = strtolower($logic);
        if ($field instanceof \Closure) {
            $this->options['where'][$logic][] = is_string($op) ? [$op, $field] : $field;
            return;
        }
        $where = [];
        if (is_null($op) && is_null($condition)) {
            if (is_array($field)) {
                // 数组批量查询
                if ($this->builder->isIndexArr($field)) {
                    foreach ($field as $v) {
                        $k = array_shift($v);
                        $where[$k] = $v;
                    }
                } else {
                    foreach ($field as $k => $v) {
                        $where[$k] = is_array($v) ? ['in', $v] : $v;
                    }
                }
            } elseif ($field) {
                throw new Exception('string conditions are not supported');
            } else {
                $where = '';
            }
        } elseif (is_array($op)) {
            $item = array_pop($param);
            if (is_string($item)) {
                $logic = strtolower($item) == 'or' ? 'should' : 'must';
            } else {
                throw new Exception('query conditions do not conform to specifications');
            }
            $where[$field] = $param;
        } elseif (in_array(strtolower($op), ['null', 'notnull', 'not null'])) {
            // null查询
            $where[$field] = [$op, ''];
        } elseif (is_null($condition)) {
            // 字段相等查询
            $where[$field] = ['=', $op];
        } else {
            if ('<>' == $op || 'nin' == $op || 'not in' == $op){
                $logic = 'must_not';
            }
            $where[$field] = [$op, $condition, isset($param[2]) ? $param[2] : null];
        }

        if (!empty($where)) {
            foreach ($where as $field => $val) {
                if (isset($this->options['where'][$logic][$field])) {
                    $tmp = $this->options['where'][$logic][$field];
                    if ($this->builder->isIndexArr($tmp)) {
                        $this->options['where'][$logic][$field][] = $val;
                    } else {
                        $this->options['where'][$logic][$field] = [$tmp, $val];
                    }
                } else {
                    $this->options['where'][$logic][$field] = $val;
                }
            }
        }
    }

    /**
     * 查询日期或者时间
     * @access public
     * @param string        $field 日期字段名
     * @param string        $op 比较运算符或者表达式
     * @param string|array  $range 比较范围
     * @return $this
     */
    public function whereTime($field, $op, $range = null)
    {
        if (is_null($range)) {
            // 使用日期表达式
            $date = getdate();
            switch (strtolower($op)) {
                case 'today':
                case 'd':
                    $range = ['today', 'tomorrow'];
                    break;
                case 'week':
                case 'w':
                    $range = 'this week 00:00:00';
                    break;
                case 'month':
                case 'm':
                    $range = mktime(0, 0, 0, $date['mon'], 1, $date['year']);
                    break;
                case 'year':
                case 'y':
                    $range = mktime(0, 0, 0, 1, 1, $date['year']);
                    break;
                case 'yesterday':
                    $range = ['yesterday', 'today'];
                    break;
                case 'last week':
                    $range = ['last week 00:00:00', 'this week 00:00:00'];
                    break;
                case 'last month':
                    $range = [date('y-m-01', strtotime('-1 month')), mktime(0, 0, 0, $date['mon'], 1, $date['year'])];
                    break;
                case 'last year':
                    $range = [mktime(0, 0, 0, 1, 1, $date['year'] - 1), mktime(0, 0, 0, 1, 1, $date['year'])];
                    break;
                default:
                    $range = $op;
            }
            $op = is_array($range) ? 'between' : '>';
        }
        $this->where($field, strtolower($op) . ' time', $range);
        return $this;
    }

    /**
     * 分页查询
     * @param int|null  $listRows 每页数量
     * @param bool      $simple 简洁模式
     * @param array     $config 配置参数
     *                      page:当前页,
     *                      path:url路径,
     *                      query:url额外参数,
     *                      fragment:url锚点,
     *                      var_page:分页变量,
     *                      list_rows:每页数量
     *                      type:分页类名,
     *                      namespace:分页类命名空间
     * @return \think\Paginator
     * @throws DbException
     */
    public function paginate($listRows = null, $simple = false, $config = [])
    {
        $config   = array_merge(Config::get('paginate'), $config);
        $listRows = $listRows ?: $config['list_rows'];
        $class    = strpos($config['type'], '\\') ? $config['type'] : '\\think\\paginator\\driver\\' . ucwords($config['type']);
        $page     = isset($config['page']) ? (int) $config['page'] : call_user_func([
            $class,
            'getCurrentPage',
        ], $config['var_page']);

        $page = $page < 1 ? 1 : $page;

        $config['path'] = isset($config['path']) ? $config['path'] : call_user_func([$class, 'getCurrentPath']);

        /** @var Paginator $paginator */
        if (!$simple) {
            $options = $this->getOptions();
            $total   = $this->count();
            $results = $this->options($options)->page($page, $listRows)->select();
        } else {
            $results = $this->limit(($page - 1) * $listRows, $listRows + 1)->select();
            $total   = null;
        }
        return $class::make($results, $listRows, $page, $total, $simple, $config);
    }

    /**
     * 指定当前操作的数据表
     * @access public
     * @param string $table 表名
     * @return $this
     */
    public function table($table)
    {
        $this->options['table'] = $table;
        return $this;
    }

    /**
     * 查询缓存
     * @access public
     * @param mixed   $key    缓存key
     * @param integer $expire 缓存有效期
     * @param string  $tag    缓存标签
     * @return $this
     */
    public function cache($key = true, $expire = null, $tag = null)
    {
        // 增加快捷调用方式 cache(10) 等同于 cache(true, 10)
        if (is_numeric($key) && is_null($expire)) {
            $expire = $key;
            $key    = true;
        }
        if (false !== $key) {
            $this->options['cache'] = ['key' => $key, 'expire' => $expire, 'tag' => $tag];
        }
        return $this;
    }

    /**
     * 设置软删除字段及条件（暂无支持）
     * @access public
     * @param false|string  $field     查询字段
     * @param mixed         $condition 查询条件
     * @return $this
     */
    public function useSoftDelete($field, $condition = null)
    {
    }

    /**
     * 不主动获取数据集
     * @access public
     * @param bool  $cursor 是否返回对象
     * @return $this
     */
    public function fetchCursor($cursor = true)
    {
        $this->options['fetch_cursor'] = $cursor;
        return $this;
    }

    /**
     * 设置从主服务器读取数据
     * @access public
     * @return $this
     */
    public function master()
    {
        $this->options['master'] = true;
        return $this;
    }

    /**
     * 设置查询数据不存在是否抛出异常
     * @access public
     * @param bool $fail 是否严格检查字段
     * @return $this
     */
    public function failException($fail = true)
    {
        $this->options['fail'] = $fail;
        return $this;
    }

    /**
     * awaitData
     * @access public
     * @param bool $awaitData
     * @return $this
     */
    public function awaitData($awaitData)
    {
        $this->options['awaitData'] = $awaitData;
        return $this;
    }

    /**
     * batchSize
     * @access public
     * @param integer $batchSize
     * @return $this
     */
    public function batchSize($batchSize)
    {
        $this->options['batchSize'] = $batchSize;
        return $this;
    }

    /**
     * exhaust
     * @access public
     * @param bool $exhaust
     * @return $this
     */
    public function exhaust($exhaust)
    {
        $this->options['exhaust'] = $exhaust;
        return $this;
    }

    /**
     * 设置modifiers
     * @access public
     * @param array $modifiers
     * @return $this
     */
    public function modifiers($modifiers)
    {
        $this->options['modifiers'] = $modifiers;
        return $this;
    }

    /**
     * 设置noCursorTimeout
     * @access public
     * @param bool $noCursorTimeout
     * @return $this
     */
    public function noCursorTimeout($noCursorTimeout)
    {
        $this->options['noCursorTimeout'] = $noCursorTimeout;
        return $this;
    }

    /**
     * 设置oplogReplay
     * @access public
     * @param bool $oplogReplay
     * @return $this
     */
    public function oplogReplay($oplogReplay)
    {
        $this->options['oplogReplay'] = $oplogReplay;
        return $this;
    }

    /**
     * 设置partial
     * @access public
     * @param bool $partial
     * @return $this
     */
    public function partial($partial)
    {
        $this->options['partial'] = $partial;
        return $this;
    }

    /**
     * 查询注释
     * @access public
     * @param string $comment 注释
     * @return $this
     */
    public function comment($comment)
    {
        $this->options['comment'] = $comment;
        return $this;
    }

    /**
     * maxTimeMS
     * @access public
     * @param string $maxTimeMS
     * @return $this
     */
    public function maxTimeMS($maxTimeMS)
    {
        $this->options['maxTimeMS'] = $maxTimeMS;
        return $this;
    }

    /**
     * 设置返回字段
     * @access public
     * @param array     $field
     * @param boolean   $except 是否排除
     * @return $this
     */
    public function field($field, $except = false)
    {
        if (is_string($field)) {
            $field = array_map('trim', explode(',', $field));
        }
        $projection = [];
        foreach ($field as $key => $val) {
            if (is_numeric($key)) {
                $projection[$val] = $except ? 0 : 1;
            } else {
                $projection[$key] = $except ? 0 : $val;
            }
        }
        $this->options['projection'] = $projection;
        return $this;
    }

    /**
     * 关联预载入查询
     * @access public
     * @param mixed $with
     * @return $this
     */
//     public function with($with)
//     {
//         $this->options['with'] = $with;
//         return $this;
//     }

    /**
     * 关联统计
     * @access public
     * @param string|array $relation 关联方法名
     * @return $this
     */
//     public function withCount($relation)
//     {
//         $this->options['with_count'] = $relation;
//         return $this;
//     }

    /**
     * 指定查询数量
     * @access public
     * @param mixed $offset 起始位置
     * @param mixed $length 查询数量
     * @return $this
     */
    public function limit($offset, $length = null)
    {
        if (is_null($length)) {
            if (is_numeric($offset)) {
                $length = $offset;
                $offset = 0;
            } else {
                list($offset, $length) = explode(',', $offset);
            }
        }
        $this->options['from']  = intval($offset);
        $this->options['limit'] = intval($length);

        return $this;
    }

    /**
     * 指定分页
     * @access public
     * @param mixed $page 页数
     * @param mixed $listRows 每页数量
     * @return $this
     */
    public function page($page, $listRows = null)
    {
        if (is_null($listRows) && strpos($page, ',')) {
            list($page, $listRows) = explode(',', $page);
        }
        $this->options['page'] = [intval($page), intval($listRows)];
        return $this;
    }

    /**
     * 指定排序 order('id','desc') 或者 order(['id'=>'desc','create_time'=>'desc'])
     * @access public
     * @param array|string|object   $field
     * @param string                $order
     * @return $this
     */
    public function order($field, $order = 'asc')
    {
        if (!isset($this->options['sort'])) {
            $this->options['sort'] = [];
        }
        if (is_array($field)) {
            foreach ($field as $key => $val) {
                if (is_numeric($key)) {
                    $this->options['sort'][] = $val;
                } else {
                    $this->options['sort'][] = [$key => $val];
                }
            }
        } else {
            $tmpArr = explode(',', $field);
            foreach ($tmpArr as $key => $val) {
                $arr = explode(' ', $val);
                $this->options['sort'][] = [$arr[0] => strtolower($arr[1] ?? $order)];
            }
        }
        return $this;
    }

    /**
     * 设置tailable
     * @access public
     * @param bool $tailable
     * @return $this
     */
    public function tailable($tailable)
    {
        $this->options['tailable'] = $tailable;
        return $this;
    }

    /**
     * 获取当前数据表的主键
     * @access public
     * @return string|array
     */
    public function getPk()
    {
        return !empty($this->pk) ? $this->pk : $this->getConfig('pk');
    }

    /**
     * 查询参数赋值
     * @access protected
     * @param array $options 表达式参数
     * @return $this
     */
    protected function options(array $options)
    {
        $this->options = $options;
        return $this;
    }

    /**
     * 获取当前的查询参数
     * @access public
     * @param string $name 参数名
     * @return mixed
     */
    public function getOptions($name = '')
    {
        return isset($this->options[$name]) ? $this->options[$name] : $this->options;
    }

    /**
     * 设置关联查询
     * @access public
     * @param string $relation 关联名称
     * @return $this
     */
    public function relation($relation)
    {
        $this->options['relation'] = $relation;
        return $this;
    }

    /**
     * 把主键值转换为查询条件 支持复合主键
     * @access public
     * @param array|string  $data 主键数据
     * @param mixed         $options 表达式参数
     * @return void
     * @throws Exception
     */
    protected function parsePkWhere($data, &$options)
    {
        $pk = $this->getPk();

        if (is_string($pk)) {
            // 根据主键查询
            if (is_array($data)) {
                $where[$pk] = isset($data[$pk]) ? $data[$pk] : ['in', $data];
            } else {
                $where[$pk] = strpos($data, ',') ? ['in', $data] : $data;
            }
        }

        if (!empty($where)) {
            if (isset($options['where']['must'])) {
                $options['where']['must'] = array_merge($options['where']['must'], $where);
            } else {
                $options['where']['must'] = $where;
            }
        }
        return;
    }

    /**
     * 插入记录
     * @access public
     * @param mixed     $data 数据
     * @param boolean   $replace      是否replace（目前无效）
     * @param boolean   $getLastInsID 返回自增主键
     * @return WriteResult
     * @throws Exception
     */
    public function insert(array $data, $replace = null, $getLastInsID = false)
    {
        // 分析查询表达式
        $options = $this->parseExpress();
        $data    = array_merge($options['data'], $data);
        
        // 生成bulk
        $bulk         = $this->builder->insert($data, $options);
        $result  = $this->execute($bulk);
        if ($result) {
            $lastInsId = $this->getLastInsID();
            is_array($lastInsId) && ($lastInsId = array_pop($lastInsId));
            if ($lastInsId) {
                $pk        = $this->getPk();
                $data[$pk] = $lastInsId;
            }
            $options['data'] = $data;
            $this->trigger('after_insert', $options);

            if ($getLastInsID) {
                return $lastInsId;
            }
        }
        return $result;
    }

    /**
     * 插入记录并获取自增ID
     * @access public
     * @param mixed $data 数据
     * @return integer
     * @throws Exception
     */
    public function insertGetId(array $data)
    {
        return $this->insert($data, null, true);
    }

    /**
     * 批量插入记录
     * @access public
     * @param mixed $dataSet 数据集
     * @return integer
     * @throws Exception
     */
    public function insertAll(array $dataSet)
    {
        // 分析查询表达式
        $options = $this->parseExpress();
        if (!is_array(reset($dataSet))) {
            return false;
        }

        // 生成bulk
        $bulk         = $this->builder->insertAll($dataSet, $options);
        return $this->execute($bulk);
    }

    /**
     * 更新记录
     * @access public
     * @param mixed $data 数据
     * @return int
     * @throws Exception
     */
    public function update(array $data = [])
    {
        $options = $this->parseExpress();
        $data    = array_merge($options['data'], $data);
        if (isset($options['cache']) && is_string($options['cache']['key'])) {
            $key = $options['cache']['key'];
        }
        $pk = $this->getPk();
        if (empty($options['where'])) {
            // 如果存在主键数据 则自动作为更新条件
            if (is_string($pk) && isset($data[$pk])) {
                $where[$pk] = $data[$pk];
                $key        = 'elastic:' . $options['table'] . '|' . $data[$pk];
                unset($data[$pk]);
            } elseif (is_array($pk)) {
                throw new Exception('complex primary keys are not supported');
                // 增加复合主键支持
//                 foreach ($pk as $field) {
//                     if (isset($data[$field])) {
//                         $where[$field] = $data[$field];
//                     } else {
//                         // 如果缺少复合主键数据则不执行
//                         throw new Exception('miss complex primary data');
//                     }
//                     unset($data[$field]);
//                 }
            }
            if (!isset($where)) {
                // 如果没有任何更新条件则不执行
                throw new Exception('miss update condition');
            } else {
                $options['where']['must'] = $where;
            }
        } elseif (!isset($key) && is_string($pk) && isset($options['where']['must'][$pk])) {
            if (isset($data[$pk])) unset($data[$pk]);
            $key = $this->getCacheKey($options['where']['must'][$pk], $options);
        }
        
        if (is_string($pk) && isset($options['where']['must'][$pk])) {
            // 有主键
            $params    = $this->builder->update($data, $options, $options['where']['must'][$pk]);
            $result  = $this->execute($params);
        } else {
            $params    = $this->builder->update($data, $options);
            $result  = $this->connection->updateByQuery($params);
        }
        // 检测缓存
        if (isset($options['cache']) && isset($key) && Cache::get($key)) {
            Cache::rm($key);
        }
        if ($result) {
            if (is_string($pk) && isset($where[$pk])) {
                $data[$pk] = $where[$pk];
            } elseif (is_string($pk) && isset($key) && strpos($key, '|')) {
                list($a, $val) = explode('|', $key);
                $data[$pk]     = $val;
            }
            $options['data'] = $data;
            $this->trigger('after_update', $options);
        }
        return $result;
    }

    /**
     * 删除记录
     * @access public
     * @param array $data 表达式 true 表示强制删除
     * @return int
     * @throws Exception
     */
    public function delete($data = null)
    {
        // 分析查询表达式
        $options = $this->parseExpress();
        $pk      = $this->getPk();
        if (isset($options['cache']) && is_string($options['cache']['key'])) {
            $key = $options['cache']['key'];
        }
        if (!is_null($data) && true !== $data) {
            if (!is_array($data)) {
                // 缓存标识
                $key = 'elastic:' . $options['table'] . '|' . $data;
            }
            // AR模式分析主键条件
            $this->parsePkWhere($data, $options);
        } elseif (!isset($key) && is_string($pk) && isset($options['where']['must'][$pk])) {
            $key = $this->getCacheKey($options['where']['must'][$pk], $options);
        }

        if (true !== $data && empty($options['where'])) {
            // 如果不是强制删除且条件为空 不进行删除操作
            throw new Exception('delete without condition');
        }

        //
        if (is_string($pk) && isset($options['where']['must'][$pk])) {
            // 有主键
            $bulk = $this->builder->delete($options, $options['where']['must'][$pk]);
            $result = $this->execute($bulk);
        } else {
            $bulk = $this->builder->delete($options);
            $result = $this->connection->deleteByQuery($bulk);
        }
        // 检测缓存
        if (isset($options['cache']) && isset($key) && Cache::get($key)) {
            Cache::rm($key);
        }
        if ($result) {
            if (!is_array($data) && is_string($pk) && isset($key) && strpos($key, '|')) {
                list($a, $val) = explode('|', $key);
                $item[$pk]     = $val;
                $data          = $item;
            }
            $options['data'] = $data;
            $this->trigger('after_delete', $options);
        }
        return $result;
    }

    /**
     * 执行查询但只返回 对象
     * @access public
     * @return mixed
     */
    public function getCursor()
    {
//         // 分析查询表达式
//         $options = $this->parseExpress();
//         // 生成MongoQuery对象
//         $query = $this->builder->select($options);
//         // 执行查询操作
//         return $this->query($query, true);
    }

    /**
     * 查找记录
     * @access public
     * @param array|string|Query|\Closure $data
     * @return Collection|false|string
     * @throws Exception
     */
    public function select($data = null)
    {
        if ($data instanceof Query) {
            return $data->select();
        } elseif ($data instanceof \Closure) {
            call_user_func_array($data, [ & $this]);
            $data = null;
        }
        // 分析查询表达式
        $options = $this->parseExpress();

        if (!is_null($data)) {
            // 主键条件分析
            $this->parsePkWhere($data, $options);
        }

        $resultSet = false;
        if (!empty($options['cache'])) {
            // 判断查询缓存
            $cache     = $options['cache'];
            $key       = is_string($cache['key']) ? $cache['key'] : md5(serialize($options));
            $resultSet = Cache::get($key);
        }
        if (!$resultSet) {
            // 生成查询结构
            $query = $this->builder->select($options);

            $options['data'] = $data;
            if ($resultSet = $this->trigger('before_select', $options)) {
            } else {
                // 执行查询操作
                $resultSet = $this->query($query, $options['fetch_cursor']);
            }
            if (isset($cache)) {
                // 缓存数据集
                $this->cacheData($key, $resultSet, $cache);
            }
        }

        // 数据列表读取后的处理
        if (!empty($this->model)) {
            // 生成模型对象
            $modelName = $this->model;
            if (count($resultSet) > 0) {
                foreach ($resultSet as $key => $result) {
                    /** @var Model $result */
                    $model = new $modelName($result);
                    $model->isUpdate(true);

                    // 关联查询
                    if (!empty($options['relation'])) {
                        $model->relationQuery($options['relation']);
                    }
                    // 关联统计
                    if (!empty($options['with_count'])) {
                        $model->relationCount($model, $options['with_count']);
                    }
                    $resultSet[$key] = $model;
                }
                if (!empty($options['with'])) {
                    // 预载入
                    $model->eagerlyResultSet($resultSet, $options['with']);
                }
                // 模型数据集转换
                $resultSet = $model->toCollection($resultSet);
            } else {
                $resultSet = (new $modelName)->toCollection($resultSet);
            }
        } elseif ('collection' == $this->connection->getConfig('resultset_type')) {
            // 返回Collection对象
            $resultSet = new Collection($resultSet);
        }
        if (!empty($options['fail']) && count($resultSet) == 0) {
            $this->throwNotFound($options);
        }
        return $resultSet;
    }

    /**
     * 缓存数据
     * @access public
     * @param string    $key    缓存标识
     * @param mixed     $data   缓存数据
     * @param array     $config 缓存参数
     */
    protected function cacheData($key, $data, $config = [])
    {
        if (isset($config['tag'])) {
            Cache::tag($config['tag'])->set($key, $data, $config['expire']);
        } else {
            Cache::set($key, $data, $config['expire']);
        }
    }

    /**
     * 生成缓存标识
     * @access public
     * @param mixed     $value   缓存数据
     * @param array     $options 缓存参数
     */
    protected function getCacheKey($value, $options)
    {
        if (is_scalar($value)) {
            $data = $value;
        } elseif (is_array($value) && '=' == $value[0]) {
            $data = $value[1];
        }
        if (isset($data)) {
            return 'elastic:' . $options['table'] . '|' . $data;
        }
    }

    /**
     * 查找单条记录
     * @access public
     * @param array|string|Query|\Closure $data
     * @return array|null|string|Model
     * @throws Exception
     */
    public function find($data = null)
    {
        if ($data instanceof Query) {
            return $data->find();
        } elseif ($data instanceof \Closure) {
            call_user_func_array($data, [ & $this]);
            $data = null;
        }
        // 分析查询表达式
        $options = $this->parseExpress();
        $pk      = $this->getPk();
        if (!is_null($data)) {
            // AR模式分析主键条件
            $this->parsePkWhere($data, $options);
        } elseif (!empty($options['cache']) && true === $options['cache']['key'] && is_string($pk) && isset($options['where']['must'][$pk])) {
            $key = $this->getCacheKey($options['where']['must'][$pk], $options);
        }

        $options['limit'] = 1;
        $result           = false;
        if (!empty($options['cache'])) {
            // 判断查询缓存
            $cache = $options['cache'];
            if (true === $cache['key'] && !is_null($data) && !is_array($data)) {
                $key = 'elastic:' . $options['table'] . '|' . $data;
            } elseif (!isset($key)) {
                $key = is_string($cache['key']) ? $cache['key'] : md5(serialize($options));
            }
            $result = Cache::get($key);
        }
        if (false === $result) {
            // 生成查询SQL
            $query = $this->builder->select($options);
            if (is_string($pk)) {
                if (!is_array($data)) {
                    if (isset($key) && strpos($key, '|')) {
                        list($a, $val) = explode('|', $key);
                        $item[$pk]     = $val;
                    } else {
                        $item[$pk] = $data;
                    }
                    $data = $item;
                }
            }
            $options['data'] = $data;
            // 事件回调
            if ($result = $this->trigger('before_find', $options)) {
            } else {
                // 执行查询
                $resultSet = $this->query($query, $options['fetch_cursor']);
                $result = isset($resultSet[0]) ? $resultSet[0] : null;
            }
            if (isset($cache)) {
                // 缓存数据
                $this->cacheData($key, $result, $cache);
            }
        }

        // 数据处理
        if (!empty($result)) {
            if (!empty($this->model)) {
                // 返回模型对象
                $model  = $this->model;
                $result = new $model($result);
                $result->isUpdate(true, isset($options['where']['must']) ? $options['where']['must'] : null);
                // 关联查询
                if (!empty($options['relation'])) {
                    $result->relationQuery($options['relation']);
                }
                if (!empty($options['with'])) {
                    // 预载入
                    $result->eagerlyResult($result, $options['with']);
                }
                // 关联统计
                if (!empty($options['with_count'])) {
                    $result->relationCount($result, $options['with_count']);
                }
            }
        } elseif (!empty($options['fail'])) {
            $this->throwNotFound($options);
        }
        return $result;
    }

    /**
     * 查询失败 抛出异常
     * @access public
     * @param array $options 查询参数
     * @throws ModelNotFoundException
     * @throws DataNotFoundException
     */
    protected function throwNotFound($options = [])
    {
        if (!empty($this->model)) {
            throw new ModelNotFoundException('model data Not Found:' . $this->model, $this->model, $options);
        } else {
            throw new DataNotFoundException('table data not Found:' . $options['table'], $options['table'], $options);
        }
    }

    /**
     * 查找多条记录 如果不存在则抛出异常
     * @access public
     * @param array|string|Query|\Closure $data
     * @return array|\PDOStatement|string|Model
     * @throws Exception
     */
    public function selectOrFail($data = null)
    {
        return $this->failException(true)->select($data);
    }

    /**
     * 查找单条记录 如果不存在则抛出异常
     * @access public
     * @param array|string|Query|\Closure $data
     * @return array|\PDOStatement|string|Model
     * @throws Exception
     */
    public function findOrFail($data = null)
    {
        return $this->failException(true)->find($data);
    }

    /**
     * 分批数据返回处理
     * @access public
     * @param integer   $count 每次处理的数据数量
     * @param callable  $callback 处理回调方法
     * @param string    $column 分批处理的字段名
     * @return boolean
     */
    public function chunk($count, $callback, $column = null)
    {
        $column    = $column ?: $this->getPk();
        $options   = $this->getOptions();
        $resultSet = $this->limit($count)->order($column, 'asc')->select();

        while (!empty($resultSet)) {
            if (false === call_user_func($callback, $resultSet)) {
                return false;
            }
            $end       = end($resultSet);
            $lastId    = is_array($end) ? $end[$column] : $end->$column;
            $resultSet = $this->options($options)
                ->limit($count)
                ->where($column, '>', $lastId)
                ->order($column, 'asc')
                ->select();
        }
        return true;
    }

    /**
     * 获取数据表信息
     * @access public
     * @param string $tableName 数据表名 留空自动获取
     * @param string $fetch 获取信息类型 包括 fields type pk
     * @return mixed
     */
    public function getTableInfo($tableName = '', $fetch = '')
    {
        if (!$tableName) {
            $tableName = $this->getTable();
        }
        if (is_array($tableName)) {
            $tableName = key($tableName) ?: current($tableName);
        }

        if (strpos($tableName, ',')) {
            // 多表不获取字段信息
            return false;
        } else {
            $tableName = $this->parseSqlTable($tableName);
        }

        $guid = md5($tableName);
        if (!isset(self::$info[$guid])) {
            $result = $this->connection->indices(['index' => $tableName, 'type' => $this->builder->esType]);
            $result = $result[$tableName]['mappings'][$this->builder->esType]['properties'] ?? [];
            $fields = array_keys($result);
            $type   = [];
            foreach ($result as $key => $val) {
                // 记录字段类型
                $type[$key] = ('date' == $val['type'] && isset($val['format']) ? $val['format'] : $val['type']);
                if ($this->getPk() == $key) {
                    $pk = $key;
                }
            }
            if (!isset($pk)) {
                // 设置主键
                $pk = null;
            }
            $result            = ['fields' => $fields, 'type' => $type, 'pk' => $pk];
            self::$info[$guid] = $result;
        }
        return $fetch ? self::$info[$guid][$fetch] : self::$info[$guid];
    }

    /**
     * 分析表达式（可用于查询或者写入操作）
     * @access protected
     * @return array
     */
    protected function parseExpress()
    {
        $options = $this->options;

        // 获取数据表
        if (empty($options['table'])) {
            $options['table'] = $this->getTable();
        }
        if (! $options['table']) {
            throw new Exception('please set the table');
        } elseif (is_array($options['table'])) {
            throw new Exception('do not operate on multiple tables');
        }

        foreach (['where', 'data'] as $name) {
            if (!isset($options[$name])) {
                $options[$name] = [];
            }
        }

        $modifiers = empty($options['modifiers']) ? [] : $options['modifiers'];
        if (isset($options['comment'])) {
            $modifiers['comment'] = $options['comment'];
        }

        if (isset($options['maxTimeMS'])) {
            $modifiers['maxTimeMS'] = $options['maxTimeMS'];
        }

        if (!empty($modifiers)) {
            $options['modifiers'] = $modifiers;
        }

        if (!isset($options['projection']) || '*' == $options['projection']) {
            $options['projection'] = [];
        }

        if (!isset($options['limit'])) {
            $options['limit'] = $this->getConfig('default_limit');
        }

        foreach (['master', 'fetch_cursor'] as $name) {
            if (!isset($options[$name])) {
                $options[$name] = false;
            }
        }

        if (isset($options['page'])) {
            $max = $this->getConfig('max_page_offset');
            // 根据页数计算limit
            list($page, $listRows) = $options['page'];
            $page                  = $page > 0 ? $page : 1;
            $listRows              = $listRows > 0 ? $listRows : (is_numeric($options['limit']) ? $options['limit'] : 20);
            $offset                = $listRows * ($page - 1);
            $offset > $max && ($offset = $max);
            $options['from']       = $offset;
            $options['limit']      = $listRows;
        }

        $this->options = [];
        return $options;
    }

    /**
     * 注册回调方法
     * @access public
     * @param string   $event    事件名
     * @param callable $callback 回调方法
     * @return void
     */
    public static function event($event, $callback)
    {
        self::$event[$event] = $callback;
    }

    /**
     * 触发事件
     * @access protected
     * @param string $event   事件名
     * @param mixed  $params  额外参数
     * @return bool
     */
    protected function trigger($event, $params = [])
    {
        $result = false;
        if (isset(self::$event[$event])) {
            $callback = self::$event[$event];
            $result   = call_user_func_array($callback, [$params, $this]);
        }
        return $result;
    }
}
