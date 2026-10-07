class RuleParse:

    def __init__(self,inType,nodes,table,selectFields,range):
        # 定义实例属性
        self.inType=inType
        self.nodes=nodes
        self.table=table
        self.selectFields=selectFields
        self.range=range

    @staticmethod
    def parse(rule):
        """
        用来解析rule规则。
        为什么定义为静态方法？
        因为该方法不需要用到实例对象，类对象
        :param rule: 需要解析的rule规则字符串
        :return: 实例对象
        """
        # 1 - 按照##切分
        rule_list=rule.split("##")

        # 2 - 定义一个空字典，用来存放解析后的内容
        new_dict = {}

        # 3 - 对列表中的每个元素继续切分，按照=切分得到key和value
        for tmp in rule_list:
            keyvalue_list=tmp.split("=")
            new_dict[keyvalue_list[0]]=keyvalue_list[1]

        # 4 - 返回结果
        # 下面的两行代码都是用来将字典的内容传递进去，然后得到类的实例对象。使用哪个都行
        # return RuleParse(new_dict["inType"],new_dict["nodes"],new_dict["table"],new_dict["selectFields"],new_dict["range"])
        return RuleParse(**new_dict)


if __name__=="__main__":
    rule_str="inType=Hive##nodes=up01:9083##table=dwd.dwd_mem_menber_union_i##selectFields=zt_id,sex##range=all"

    result_obj=RuleParse.parse(rule_str)
    print(result_obj.inType)
    print(result_obj.nodes)
    print(result_obj.table)