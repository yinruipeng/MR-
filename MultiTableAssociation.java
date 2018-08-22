static int row = 0 ;//表头标记
	// 输出key是 单词+文件名,输出value为次数1
	public static class ExtractTVMsgLogMapper extends Mapper<LongWritable,Text,Text,Text>{
		
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String tag = new String();//左右表标示
			String mapkey = new String();
			String mapvalue = new String();
			int i = 0;//判断读取到的是左表还是右表
			//略过文件首行
			if(line.contains("factoryname") || line.contains("addressed"))
				return;
			StringTokenizer st = new StringTokenizer(line);
			while(st.hasMoreTokens()){
				String word = st.nextToken();
				//如果先读取到addressId就把id作为key
				if(word.charAt(0)>='0' && word.charAt(0)<='9'){
					mapkey = word;//先读到id说明是右表 存储的是addressname
					if(i==0){
						tag = "R";
					}else{
						tag = "L";
					}
					continue;
				}
        //读到id之外的其他的内容
				mapvalue += word+" ";
				i++;

			}
			context.write(new Text(mapkey), new Text(tag+"+"+mapvalue));
		}
	}
  
  
  
  //第二次reduce合并上一步相同的key
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			if(row == 0){ 
				context.write(new Text("factoryname"), new Text("addressname")); 
				row++; 
			} 
			//根据左右表标识提取对应信息 
			int factoryNum = 0; 
			int addressNum = 0; 
			String[] factory = new String[20]; 
			String[] address = new String[20]; 
			for(Text value : values){ 
				// 左表存的factoryname 
				char tag = value.toString().charAt(0); 
				if(tag == 'L'){
					System.out.println("-------Lsubstring(2)--------"+value.toString());
					factory[factoryNum++] = value.toString().substring(2); 
				} 
        if(tag == 'R'){ 
					address[addressNum++] = value.toString().substring(2); 
					System.out.println("-------R substring(2)--------"+value.toString());
				} 
			} 
			if(factoryNum > 0 && addressNum >0){
				for(int i = 0; i < factoryNum; i++){ 
					for(int j = 0; j < addressNum; j++){ 
						context.write(new Text(factory[i]), new Text(address[j])); 
					} 
				} 
			} 
		}
	}
  
  
