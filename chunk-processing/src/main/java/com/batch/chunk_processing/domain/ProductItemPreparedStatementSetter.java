package com.batch.chunk_processing.domain;

import org.springframework.batch.item.database.ItemPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ProductItemPreparedStatementSetter implements ItemPreparedStatementSetter<Product> {
    @Override
    public void setValues(Product product, PreparedStatement ps) throws SQLException {
        ps.setInt(1, product.getProductId());
        ps.setString(2, product.getProductName());
        ps.setString(3, product.getProductCategory());
        ps.setInt(4, product.getProductPrice());

    }
}
