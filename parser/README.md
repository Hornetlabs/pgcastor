# postgresql-blast-c
## 代码目录
```
--- postgresql-blast-c
      |---- src        源码目录
      |       |---- datapage       postgresql页机制相关
      |       |---- errno          错误码相关
      |       |---- jniinterface   jni接口相关
      |       |---- log            日志相关
      |       |---- mcxt           内存页相关
      |       |---- sysdict        系统表相关
      |       |---- thirdparty     第三方库相关
      |       |---- trans          解析接口相关
      |             |---- chkpoint     checkpoint解析
      |             |---- ddlstmt      ddl解析
      |             |---- dmlstmt      dml解析
      |             |---- trans        预解析
      |
      |----include  头文件目录
              |---- common        通用头文件
              |---- datapage      postgresql页机制头文件
              |---- errno         错误码头文件
              |---- log           日志头文件
              |---- mcxt          内存页头文件
              |---- sysdict       系统表头文件
              |---- trans         解析接口头文件
                    |---- chkpoint     checkpoint解析头文件
                    |---- ddlstmt      ddl解析头文件
                    |---- dmlstmt      dml解析头文件
                    |---- trans        预解析头文件
```
