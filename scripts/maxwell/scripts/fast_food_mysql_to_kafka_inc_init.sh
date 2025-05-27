#!/bin/bash

# 该脚本的作用是初始化所有的增量表，只需执行一次

MAXWELL_HOME=/opt/module/maxwell

import_data() {
    for tab in $@
    do
        echo "正在执行 ${tab} 表的全表扫描..."
        $MAXWELL_HOME/bin/maxwell-bootstrap --database fast_food --table ${tab} --config $MAXWELL_HOME/config.properties
        echo "执行完毕~"
    done
}

case $1 in
customer | order_info | order_detail | order_status_log | payment)
    import_data $1
    ;;
"all")
    import_data customer order_info order_detail order_status_log payment
    ;;
esac
