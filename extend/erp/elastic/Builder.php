<?php
/**
 * Copyright: Foudaful
 * Author: sky <sky.nie@qq.com>
 * Date: 2019/10/07 09:00
 **/

namespace erp\elastic;

use think\Exception;

class Builder
{
    public $esType = '_doc';
    // connection对象实例
    protected $connection;
    // 查询对象实例
    protected $query;
    // 查询参数
    protected $options = [];
    // 最后插入ID
    protected $insertId = [];
    // 多个查询条件
    protected $isMulti = null;
    // 查询表达式
    protected $exp = ['ne' => '<>', 'eq' => '=', '>' => 'gt', '>=' => 'gte', '<' => 'lt', '<=' => 'lte', 'like' => 'like', 'in' => 'in', 'not in' => 'nin', 'nin' => 'nin', 'between' => 'between', 'not between' => 'not between', 'exp' => 'exp', 'exists' => 'exists', 'null' => 'null', 'notnull' => 'not null', 'not null' => 'not null', '> time' => '> time', '< time' => '< time', 'between time' => 'between time', 'not between time' => 'not between time', 'notbetween time' => 'not between time'];
    /**
     * 架构函数
     * @access public
     * @param Connection    $connection 数据库连接对象实例
     * @param Query         $query 数据库查询对象实例
     */
    public function __construct(Connection $connection, Query $query)
    {
        $this->connection   = $connection;
        $this->query        = $query;
        $this->esType       = $this->connection->getConfig('es_type');
    }
    
    public function isIndexArr(array $arr)
    {
        $keys = array_keys($arr);
        return $keys == array_keys($keys);
    }

    /**
     * key分析
     * @access protected
     * @param string $key
     * @return string
     */
    protected function parseKey($key)
    {
//         if (0 === strpos($key, '__TABLE__.')) {
//             list($collection, $key) = explode('.', $key, 2);
//         }
//         if ('id' == $key && $this->connection->getConfig('pk_convert_id')) {
//             $key = '_id';
//         }
        return trim($key);
    }

    /**
     * value分析
     * @access protected
     * @param mixed     $value
     * @param string    $field
     * @return string
     */
    protected function parseValue($value, $field = '')
    {
//         if ('_id' == $field && 'ObjectID' == $this->connection->getConfig('pk_type') && is_string($value)) {
//             return new ObjectID($value);
//         }
        return $value;
    }

    /**
     * insert数据分析
     * @access protected
     * @param array $data 数据
     * @param array $options 查询参数
     * @return array
     */
    protected function parseData(array $data, array $options, &$body)
    {
        if (empty($data)) {
            return [];
        }

        $result = [];
        foreach ($data as $key => $val) {
            $item = $this->parseKey($key);
            if (is_object($val)) {
                $result[$item] = $val;
            } elseif (isset($val[0]) && 'exp' == $val[0]) {
                $result[$item] = $val[1];
            } elseif (is_null($val)) {
                $result[$item] = 'NULL';
            } else {
                $result[$item] = $this->parseValue($val, $key);
            }
        }
        $data = null;
        //
        $meta = [
            'index' => [
                '_index' => $options['table'],
                '_type' => $this->esType
            ]
        ];
        isset($result['id']) && ($meta['index']['_id'] = $result['id']);
        $body[] = $meta;
        $body[] = $result;
    }

    /**
     * Set数据分析
     * @access protected
     * @param array $data 数据
     * @param array $options 查询参数
     * @return array
     */
    protected function parseSet($data, $options)
    {
        if (empty($data)) {
            return '';
        }

        $result = '';
        foreach ($data as $key => $val) {
            $result .= is_int($val) ? "ctx._source.$key=$val;" : "ctx._source.$key='".$val."';";
        }
        return $result;
    }
    
    protected function assembleData(string $logic, array $res, &$filter)
    {
        if ($this->isMulti) {
            foreach ($res as $tmpv) {
                if (key_exists('match_phrase', $tmpv) || key_exists('match_phrase_prefix', $tmpv)) {
                    $filter['bool'][$logic][] = $tmpv;
                } else {
                    $filter['bool']['filter']['bool'][$logic][] = $tmpv;
                }
            }
        } else {
            if (key_exists('match_phrase', $res) || key_exists('match_phrase_prefix', $res)) {
                $filter['bool'][$logic][] = $res;
            } else {
                $filter['bool']['filter']['bool'][$logic][] = $res;
            }
        }
        $this->isMulti = false;
    }

    /**
     * 生成查询过滤条件
     * @access public
     * @param mixed $where
     * @return array
     */
    public function parseWhere($where, $options = [])
    {
        if (empty($where)) {
            return ["match_all" => (object)[]];
        }
        
        $filter = [];
        foreach ($where as $logic => $val) {
            foreach ($val as $field => $value) {
                if ($value instanceof \Closure) {
                    // 使用闭包查询
//                     $query = new Query($this->connection);
//                     call_user_func_array($value, [ & $query]);
//                     $filter[$logic][] = $this->parseWhere($query->getOptions('where'), $options);
                } else {
                    if (strpos($field, '|')) {
                        // 不同字段使用相同查询条件（OR）
                        $array = explode('|', $field);
                        foreach ($array as $k) {
                            $res = $this->parseWhereItem($k, $value);
                            $this->assembleData('should', $res, $filter);
                        }
                    } elseif (strpos($field, '&')) {
                        // 不同字段使用相同查询条件（AND）
                        $array = explode('&', $field);
                        foreach ($array as $k) {
                            $res = $this->parseWhereItem($k, $value);
                            $this->assembleData('must', $res, $filter);
                        }
                    } else {
                        // 对字段使用表达式查询
                        $field = is_string($field) ? $field : '';
                        $res = $this->parseWhereItem($field, $value);
                        $this->assembleData($logic, $res, $filter);
                    }
                }
            }
        }

//         if (!empty($options['soft_delete'])) {
//             // 附加软删除条件
//             list($field, $condition) = $options['soft_delete'];
//             $filter['must'][] = $this->parseWhereItem($field, $condition);
//         }

        return $filter;
    }

    // where子单元分析
    protected function parseWhereItem($field, $val)
    {
        $field = $field ? trim($field) : '';
        // 查询规则和条件
        if (!is_array($val)) {
            $val = ['=', $val];
        }
        list($exp, $value) = $val;

        // 对一个字段使用多个查询条件
        if (is_array($exp)) {
            foreach ($val as $item) {
                $str[]    = $this->parseWhereItem($field, $item);
            }
            $this->isMulti = true;
            return $str;
        } elseif (!in_array($exp, $this->exp)) {
            $exp = strtolower($exp);
            if (isset($this->exp[$exp])) {
                $exp = $this->exp[$exp];
            } else {
                throw new Exception('where express error:' . $exp);
            }
        }
        
        $query = [];
        if ('=' == $exp || '<>' == $exp) {
            // 普通查询
            $query['term'] = [$field => $this->parseValue($value, $field)];
        } elseif (in_array($exp, ['gt', 'gte', 'lt', 'lte'])) {
            // 比较运算
            $query['range'] = [$field => [$exp => $this->parseValue($value, $field)]];
        } elseif ('null' == $exp) {
            // NULL查询
            $query['missing'] = ['field' => $field];
        } elseif ('notnull' == $exp || 'not null' == $exp || 'exists' == $exp) {
            // 字段是否存在
            $query['exists'] = ['field' => $field];
        } elseif ('between' == $exp) {
            // 区间查询
            $value       = is_array($value) ? $value : explode(',', $value);
            $query['range'] = [
                $field => [
                    'gte' => $value[0],
                    'lte' => $value[1]
                ]];
        } elseif ('not between' == $exp) {
            // 范围查询
            $value       = is_array($value) ? $value : explode(',', $value);
            $query['range'] = [
                $field => [
                    'lt' => $value[0],
                    'gt' => $value[1]
                ]];
        } elseif ('exp' == $exp) {
            // 表达式查询
            //$query['$where'] = $value instanceof Javascript ? $value : new Javascript($value);
        } elseif ('like' == $exp) {
            // 模糊查询
            $value = trim($value);
            $type = 0;
            $type += substr($value, 0, 1) == '%' ? 1 : 0;
            $type += substr($value, -1) == '%' ? 2 : 0;
            switch ($type) {
                case 1:
                case 3:
                    $query['match_phrase'] = [$field => ['query' => $value]];
                    break;
                case 2:
                    $query['match_phrase_prefix'] = [$field => ['query' => $value]];
                    break;
                default:
                    $query['term'] = [$field => $value];
                    break;
            }
        } elseif ('in' == $exp || 'nin' == $exp || 'not in' == $exp) {
            // IN 查询
            $value = is_array($value) ? $value : explode(',', $value);
            $query['terms'] = [$field => $this->parseValue($value, $field)];
        } elseif ('regex' == $exp) {
            //$query[$field] = $value instanceof Regex ? $value : new Regex($value, 'i');
        } elseif ('< time' == $exp) {
            $query['range'] = [$field => ['lt' => $this->parseDateTime($value, $field)]];
        } elseif ('> time' == $exp) {
            $query['range'] = [$field => ['gt' => $this->parseDateTime($value, $field)]];
        } elseif ('between time' == $exp) {
            // 区间查询
            $value       = is_array($value) ? $value : explode(',', $value);
            $query['range'] = [
                $field => [
                    'gte' => $this->parseDateTime($value[0], $field),
                    'lte' => $this->parseDateTime($value[1], $field)
                ]];
        } elseif ('not between time' == $exp) {
            // 范围查询
            $value       = is_array($value) ? $value : explode(',', $value);
            $query['range'] = [
                $field => [
                    'lt' => $this->parseDateTime($value[0], $field),
                    'gt' => $this->parseDateTime($value[1], $field)
                ]];
        } else {
            // 普通查询
            $query['term'] = [$field => $this->parseValue($value, $field)];
        }
        return $query;
    }

    /**
     * 日期时间条件解析
     * @access protected
     * @param string $value
     * @param string $key
     * @return string
     */
    protected function parseDateTime($value, $key)
    {
        // 获取时间字段类型
        $type = $this->query->getTableInfo('', 'type');
        if (isset($type[$key])) {
            $value = strtotime($value) ?: $value;
            if ('date' == $type[$key]) {
                // 日期类型
                $value = date('Y-m-d', $value);
            } elseif('yyyy-MM-dd HH:mm:ss' == $type[$key]) {
                // 日期及时间类型
                $value = date('Y-m-d H:i:s', $value);
            }
            // epoch_second
        }
        return $value;
    }
    
    /**
     * field分析
     * @access protected
     * @param mixed     $fields
     * @param array     $options
     * @return string
     */
    protected function parseField($fields, &$bulk)
    {
        if (is_array($fields)) {
            // 不支持 'field1'=>'field2' 这样的字段别名定义
            foreach ($fields as $field => $val) {
                if (0 == $val) {
                    $bulk['_source_excludes'][] = $this->parseKey($field, $options);
                } else {
                    $bulk['_source'][] = $this->parseKey($field, $options);
                }
            }
        }
    }
    
    /**
     * limit分析
     * @access protected
     * @param mixed $lmit
     * @return string
     */
    protected function parseLimit($options, &$bulk)
    {
        isset($options['from']) && ($bulk['from'] = $options['from']);
        empty($options['limit']) || ($bulk['size'] = $options['limit']);
    }
    
    /**
     * order分析
     * @access protected
     * @param mixed $order
     * @param array $options 查询条件
     * @return string
     */
    protected function parseOrder($order, &$bulk)
    {
        empty($order['sort']) || ($bulk['sort'] = $order['sort']);
    }

    /**
     * 生成insert bulk数组
     * @access public
     * @param array     $data 数据
     * @param array     $options 表达式
     * @return array
     */
    public function insert(array $data, array $options = [])
    {
        $bulk = [
            'index' => $options['table'],
            'type' => $this->esType,
            'body' => []
        ];
        // 分析并处理数据
        $this->parseData($data, $options, $bulk['body']);
        
        $this->log('insert', $bulk);
        return $bulk;
    }

    /**
     * 生成insertall bulk数组
     * @access public
     * @param array     $dataSet 数据集
     * @param array     $options 参数
     * @return array
     */
    public function insertAll(array $dataSet, array $options = [])
    {
        $bulk = [
            'index' => $options['table'],
            'type' => $this->esType,
            'body' => []
        ];
        foreach ($dataSet as $data) {
            // 分析并处理数据
            $this->parseData($data, $options, $bulk['body']);
        }
        
        $this->log('insert', $bulk, $options);
        return $bulk;
    }

    /**
     * 生成update 数据
     * @access public
     * @param array     $data 数据
     * @param array     $options 参数
     * @return array
     */
    public function update($data, $options = [], $pkValue = null)
    {
        if ($pkValue) {
            $id = is_array($pkValue) && '=' == $pkValue[0] ? $pkValue[1] : $pkValue;
            $bulk = [
                'index' => $options['table'],
                'type'  => $this->esType,
                'body' => [
                    [
                        'update' => [
                            '_index' => $options['table'],
                            '_type'  => $this->esType,
                            '_id'   => $id
                        ]
                    ],
                    ['doc' => $data]
                ]
            ];
        } else {
            $where = $this->parseWhere($options['where'], $options);
            $bulk = [
                'index' => $options['table'],
                'type'  => $this->esType,
                'body'  => [
                    'query' => $where,
                    'script'   => ['source' => $this->parseSet($data, $options)]
                ]
            ];
            $this->parseLimit($options, $bulk);
        }
        
        $this->log('update', $bulk);
        return $bulk;
    }

    /**
     * 生成delete bulk数组
     * @access public
     * @param array     $options 参数
     * @return array
     */
    public function delete($options, $pkValue = null)
    {
        if ($pkValue) {
            $id = is_array($pkValue) && '=' == $pkValue[0] ? $pkValue[1] : $pkValue;
            $bulk = [
                'index' => $options['table'],
                'type'  => $this->esType,
                'body'  => [
                    [
                        'delete' => [
                            '_index' => $options['table'],
                            '_type'  => $this->esType,
                            '_id'   => $id
                        ]
                    ]
                ]
            ];
        } else {
            $where = $this->parseWhere($options['where'], $options);
            $bulk = [
                'index' => $options['table'],
                'type'  => $this->esType,
                'body'  => ['query' => $where]
            ];
            $this->parseLimit($options, $bulk);
        }
        
        $this->log('remove', $bulk);
        return $bulk;
    }

    /**
     * 生成查询
     * @access public
     * @param array $options 参数
     * @return array
     */
    public function select($options)
    {
        $where = $this->parseWhere($options['where'], $options);
        $query = [
            'index' => $options['table'],
            'type'  => $this->esType,
            'body'  => ['query' => $where]
        ];
        $this->parseField($options['projection'], $query);
        $this->parseLimit($options, $query);
        $this->parseOrder($options, $query);
        
        $this->log('find', $query);
        return $query;
    }

    /**
     * 聚合查询命令
     * @access public
     * @param array $options 参数
     * @param array $extra   指令和字段
     * @return array
     */
    public function aggregate($options, $extra)
    {
        list($cmd, $field) = $extra;
        $where = $this->parseWhere($options['where'], $options);
        $op = [
            'index' => $options['table'],
            'type'  => $this->esType,
            'size'  => 0,
            'body'  => [
                'query' => $where,
                "aggs"  => ['agg_result' => [$cmd => ["field" => $field]]]
            ]
        ];
        $this->log('aggregate', $op);
        return $op;
    }

    /**
     * 多聚合查询命令, 可以对多个字段进行 group by 操作
     *
     * @param array $options 参数
     * @param array $extra 指令和字段
     * @return mixed
     */
//     public function multiAggregate($options, $extra)
//     {
//         list($aggregate, $groupBy) = $extra;
//         $groups                    = ['_id' => []];
//         foreach ($groupBy as $field) {
//             $groups['_id'][$field] = '$' . $field;
//         }

//         foreach ($aggregate as $fun => $field) {
//             $groups[$field . '_' . $fun] = ['$' . $fun => '$' . $field];
//         }
//         $pipeline = [
//             ['$match' => (object) $this->parseWhere($options['where'], $options)],
//             ['$group' => $groups],
//         ];
//         $cmd = [
//             'aggregate'    => $options['table'],
//             'allowDiskUse' => true,
//             'pipeline'     => $pipeline,
//         ];

//         foreach (['explain', 'collation', 'bypassDocumentValidation', 'readConcern'] as $option) {
//             if (isset($options[$option])) {
//                 $cmd[$option] = $options[$option];
//             }
//         }
//         $command = new Command($cmd);
//         $this->log('group', $cmd);
//         return $command;
//     }

    /**
     * 查询数据表的状态信息
     * @access public
     * @return mixed
     */
//     public function collStats($options)
//     {
//         $cmd     = ['collStats' => $options['table']];
//         $command = new Command($cmd);
//         $this->log('cmd', 'collStats', $cmd);
//         return $command;
//     }

    protected function log($type, $data, $options = [])
    {
        if ($this->connection->getConfig('debug')) {
            $this->connection->log($type, $data, $options);
        }
    }
}
