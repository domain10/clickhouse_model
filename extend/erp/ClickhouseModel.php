<?php
/**
 * Copyright: Foudaful
 * Author: sky <sky.nie@qq.com>
 * Date: 2021/05/19 09:30
 **/

namespace erp;

use think\Config;
use think\Model;
use think\Db;

abstract class ClickhouseModel extends Model
{
    /**
     *  初始化
     * @access protected
     * @return void
     */
//     protected static function init()
//     {
        
//     }

    /**
     * 获取当前模型的数据库查询对象
     * @access public
     * @param bool $baseQuery 是否调用全局查询范围
     * @return Query
     */
    public function db($baseQuery = true)
    {
        $config = Config::get('ch_config');
        $model = $this->class;
        if (!isset(self::$links[$model])) {
            // 合并数据库配置
            if (!empty($this->connection)) {
                if (is_array($this->connection)) {
                    $connection = array_merge($config, $this->connection);
                } else {
                    $connection = $this->connection;
                }
            } else {
                $connection = $config;
            }
            // 设置当前模型 确保查询返回模型对象
            $query = Db::connect($connection)->getQuery($model, $this->query);

            // 设置当前数据表和模型名
            if (!empty($this->table)) {
                $query->setTable($this->table);
            } else {
                $query->name($this->name);
            }

            if (!empty($this->pk)) {
                $query->pk($this->pk);
            }

            self::$links[$model] = $query;
        }
        // 全局作用域
        if ($baseQuery && method_exists($this, 'base')) {
            call_user_func_array([$this, 'base'], [ & self::$links[$model]]);
        }
        
        // 返回当前模型的数据库查询对象
        return self::$links[$model];
    }
}
