class reusable:

    var_biresh='hellow'
    #* will simply unpacked the liost in the form of normal string
    #when give mutiple column name, it will keep the list as it is and remove starting and ending brackets from that parameter and pass it to the drop column. This is called unpacking of list.
    #It will do on its own. No need to write logic
    #Tips if a list is in string format, you can convert it into list format by using eval() function
    def dropColumns(self, df, columns):
        df=df.drop(*columns) 
        return df