// 输出key是 单词+文件名,输出value为次数1
	public static class ExtractTVMsgLogMapper extends Mapper<LongWritable,Text,Text,Text>{
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			//获取数据所在文件名
			FileSplit split = (FileSplit) context.getInputSplit();
			String filename = split.getPath().getName();
			StringTokenizer st = new StringTokenizer(value.toString());
			while(st.hasMoreTokens()){
				String word = st.nextToken();
				//单词+文件名作为key，value次数为1作为输出
				String mapKey = word + "+" +filename;
				
				context.write(new Text(mapKey), new Text("1"));
			}
		}
	}
  
  /**
	 * / combine相当于第一次reduce,似乎combine的输出类型必须和map的输出一样(不太确定) ,也会有shuffle过程。
	 */
	
	public static class Combine extends Reducer<Text,Text,Text,Text>{
		protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			String[] words = StringUtils.split(key.toString(), "+");
			System.out.println("---------------"+key.toString());
			System.out.println("-------words[0]:--------"+words[0]);
			System.out.println("-------words[1]:--------"+words[1]);
			String reducekey = words[0];
			//提取文件名和出现次数作为value
			int sum = 0;
			for(Text value:values){
				sum += Integer.parseInt(value.toString());
			}
			
			String reduceValue = words[1]+"+"+sum;
			context.write(new Text(reducekey), new Text(reduceValue));
		}
		
	}
  
  //第二次reduce合并上一步相同的key
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			String reduceValue = new String();
			for(Text value:values){
				reduceValue += value.toString()+" ";
			}
			context.write(key, new Text(reduceValue));
		}
	}
