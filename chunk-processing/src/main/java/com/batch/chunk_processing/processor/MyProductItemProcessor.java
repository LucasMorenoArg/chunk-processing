package com.batch.chunk_processing.processor;

import com.batch.chunk_processing.domain.OSProduct;
import com.batch.chunk_processing.domain.Product;
import org.springframework.batch.item.ItemProcessor;


public class MyProductItemProcessor implements ItemProcessor<Product, OSProduct> {

    @Override
    public OSProduct process(Product product) {
        OSProduct osProduct = new OSProduct();
        try {
            System.out.println("processor() executed");

            osProduct.setProductId(product.getProductId());
            osProduct.setProductPrice(osProduct.getProductPrice());
            osProduct.setProductCategory(product.getProductCategory());
            osProduct.setProductName(product.getProductName());
            osProduct.setTaxPercent(product.getProductCategory().equals("Sport Accesories") ? 5 : 18);
            osProduct.setSku(product.getProductCategory().substring(0,3) + product.getProductId());
            osProduct.setTaxPercent(product.getProductPrice() < 1000 ? 75 :0);

        }catch (Exception e){
            e.printStackTrace();
            e.getLocalizedMessage();
        }
        return osProduct;
    }
}
