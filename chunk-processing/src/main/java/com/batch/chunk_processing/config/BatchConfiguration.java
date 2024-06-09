package com.batch.chunk_processing.config;

import com.batch.chunk_processing.domain.Product;
import com.batch.chunk_processing.domain.ProductRowMapper;
import com.batch.chunk_processing.domain.ProductSetFieldMapper;
import com.batch.chunk_processing.reader.ProductNameItemReader;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
	
	@Autowired
	public JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	public ItemReader<String> itemReader(){
		List<String> productList = new ArrayList<>();
		productList.add("Product 1");
		productList.add("Product 2");
		productList.add("Product 3");
		productList.add("Product 4");
		productList.add("Product 5");
		productList.add("Product 6");
		productList.add("Product 7");
		productList.add("Product 8");

		return new ProductNameItemReader(productList);
	}
	@Bean
	public ItemReader<Product> flatFileItemReader(){
		FlatFileItemReader<Product> itemReader= new FlatFileItemReader<>();
		itemReader.setLinesToSkip(1);
		itemReader.setResource(new ClassPathResource("/data/Product_Details.csv"));
		DefaultLineMapper<Product> lineMapper = new DefaultLineMapper<>();
		DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
		lineTokenizer.setNames("product_id","product_name","product_category","product_price");
		lineMapper.setLineTokenizer(lineTokenizer);
		lineMapper.setFieldSetMapper(new ProductSetFieldMapper());
		itemReader.setLineMapper(lineMapper);

		return itemReader;
	}

	@Bean
	public ItemReader<Product> jdbcItemReader(){
		JdbcCursorItemReader<Product> itemReader = new JdbcCursorItemReader<>();
		itemReader.setDataSource(dataSource);
		itemReader.setSql("select * from product_details order by product_id");
		itemReader.setRowMapper(new ProductRowMapper());
		return itemReader;
	}
	
	@Bean
	public Step step1() {
		return stepBuilderFactory.get("chunkBasedStep1")
				.<Product,Product>chunk(3)
				//.reader(flatFileItemReader())
				.reader(jdbcItemReader())
				.writer(new ItemWriter<Product>() {
					@Override
					public void write(List<? extends Product> list) throws Exception {
						System.out.println("Chunk-processing Started");
						list.forEach(System.out::println);
						System.out.println("Chunk-processing Ended");
					}
				})
				.build();
	}
	
	@Bean
	public Job firstJob() {
		return this.jobBuilderFactory.get("job1")
				.start(step1())
				.build();
	}
}
