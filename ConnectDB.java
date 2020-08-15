package Connection;

import BL.*;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Types;
import java.util.Date;
import java.sql.ResultSet;

/**
 *
 * @author monitor.gzpx.csg.cn
 */
public class ConnectDB {
   
    private static Connection con;
    private static final String driver="com.mysql.jdbc.Driver";
    private static final String user_app="gz_user";
    private static final String pass_app="GzSQLOppo@123";
    private static final String url_app="jdbc:mysql://10.115.133.135:3306/app_ad?user=gz_user";

    //Inserts
    
    public void insertCategory(category pCategory) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call insert_category(?)}");
            stmt.setString(1, pCategory.getName());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertChat(chat pChat) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call insert_chat(?)}");
            stmt.setInt(1, pChat.getId_order());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertChatMessage(chat_message p) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call insert_chat_message(?,?,?)}");
            stmt.setString(1, p.getMessage());
            stmt.setString(2, p.getUsername_writer());
            stmt.setInt(3, p.getId_chat());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertDeliveryType(delivery_type dt) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call insert_delivery_type(?)}");
            stmt.setString(1, dt.getDescription());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertOrder(order o) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call insert_order(?,?,?,?)}");
            stmt.setFloat(1, o.getPrice());
            stmt.setInt(2, o.getQuantity());
            stmt.setString(3, o.getUser_buyer());
            stmt.setInt(4, o.getId_product());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertPaymentMethod(payment_method pm) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call insert_paymentMethod(?)}");
            stmt.setString(1, pm.getDescription());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertPicture(picture p) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call insert_picture(?)}");
            stmt.setString(1, p.getPath());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertProduct(product p) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call insert_product(?,?,?,?,?,?,?,?)}");
            stmt.setFloat(1, p.getPrice());
            stmt.setString(2, p.getName());
            stmt.setString(3, p.getDescription());
            stmt.setInt(4, p.getQuant_in_stock());
            stmt.setBoolean(5, p.isIs_visible());
            stmt.setInt(6, p.getId_category());
            stmt.setString(7, p.getUsername_seller());
            stmt.setInt(8, p.getId_delivery_type());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertProductReview(product_review p) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call insert_productReview(?,?,?,?)}");
            stmt.setFloat(1, p.getScore());
            stmt.setString(2, p.getComment());
            stmt.setString(3, p.getUsername_writer());
            stmt.setInt(4, p.getIdProduct());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertReviewType(review_type p) {
        
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call insert_reviewType(?)}");
            stmt.setString(1, p.getName());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertShoppingCart(shopping_cart p) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call insert_shoppingCart(?,?,?)}");
            stmt.setString(1, p.getUsername());
            stmt.setInt(2, p.getId_product());
            stmt.setInt(3, p.getQuantity());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertUser(user u) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call insert_user(?,?,?)}");
            stmt.setString(1, u.getUsername());
            stmt.setString(2, u.getPassword());
            stmt.setInt(3, u.getId_user_type());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertUserReview(user_review u) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call insert_userReview(?,?,?,?,?)}");
            stmt.setFloat(1, u.getScore());
            stmt.setString(2, u.getComment());
            stmt.setInt(3, u.getId_type_review());
            stmt.setString(4, u.getUsername_writer());
            stmt.setString(5, u.getUsername_receiver());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertUserType(user_type ut) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call insert_userType(?)}");
            stmt.setString(1, ut.getName());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insert_userXpaymentMethod(user_X_paymentMethod up) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call insert_userXpaymentMethod(?,?)}");
            stmt.setString(1, up.getUsername());
            stmt.setInt(2, up.getId_payment_method());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertWishList(wish_list w) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call insert_wishList(?,?,?)}");
            stmt.setString(1, w.getUsername());
            stmt.setInt(2, w.getId_product());
            stmt.setInt(3, w.getQuantity());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertCity(city c) {
        try{
            con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call `insert_city`(?,?)}");
            stmt.setString(1, c.getName());
            stmt.setInt(2, c.getId_state());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertCountry(country p) {
        try{
            con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call `insert_country`(?)}");
            stmt.setString(1, p.getName());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertDistrict(district p) {
        try{
            con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call `insert_district`(?,?)}");
            stmt.setString(1, p.getName());
            stmt.setInt(2, p.getId_city());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertGender(gender p) {
        try{
            con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call `insert_gender`(?)}");
            stmt.setString(1, p.getName());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertNationality(nationality p) {
        try{
            con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call `insert_nationality`(?)}");
            stmt.setString(1, p.getName());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertPerson(person p) {
        try{
            con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call `insert_person`(?,?,?,?,?,?,?,?,?,?,?)}");
            stmt.setString(1, p.getId());
            stmt.setString(2, p.getFirst_name());
            stmt.setString(3, p.getMiddle_name());
            stmt.setString(4, p.getLast_name());
            stmt.setString(5, p.getEmail());
            stmt.setString(6, p.getPhone_number());
            stmt.setDate(7, p.getBirthday());
            stmt.setString(8, p.getPicture_path());
            stmt.setInt(9, p.getId_gender());
            stmt.setInt(10, p.getId_district());
            stmt.setString(11, p.getUsername());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertPersonXNationality(person_X_nationality p) {
        try{
            con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call `insert_person_x_nationality`(?,?)}");
            stmt.setString(1, p.getId_person());
            stmt.setInt(2, p.getId_nationality());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void insertState(state p) {
        try{
            con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call `insert_state`(?,?)}");
            stmt.setString(1, p.getName());
            stmt.setInt(2, p.getId_country());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    // Updates
    
    public void updateCategory(category pCategory) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call update_category(?,?)}");
            stmt.setInt(1, pCategory.getId());
            stmt.setString(2, pCategory.getName());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updateChat(chat pChat) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call update_chat(?,?)}");
            stmt.setInt(1, pChat.getId());
            stmt.setInt(2, pChat.getId_order());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updateChatMessage(chat_message p) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call update_chat_message(?,?,?)}");
            stmt.setInt(1, p.getId());
            stmt.setString(2, p.getMessage());
            stmt.setString(3, p.getUsername_writer());
            stmt.setInt(4, p.getId_chat());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updateDeliveryType(delivery_type dt) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call update_delivery_type(?,?)}");
            stmt.setInt(1, dt.getId());
            stmt.setString(2, dt.getDescription());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updateOrder(order o) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call update_order(?,?,?,?,?)}");
            stmt.setInt(1, o.getId());
            stmt.setFloat(2, o.getPrice());
            stmt.setInt(3, o.getQuantity());
            stmt.setString(4, o.getUser_buyer());
            stmt.setInt(5, o.getId_product());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updatePaymentMethod(payment_method pm) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call update_paymentMethod(?,?)}");
            stmt.setInt(1, pm.getId());
            stmt.setString(2, pm.getDescription());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updatePicture(picture p) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call update_picture(?)}");
            stmt.setInt(1, p.getId());
            stmt.setString(2, p.getPath());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updateProduct(product p) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call update_product(?,?,?,?,?,?,?,?,?)}");
            stmt.setInt(1, p.getId());
            stmt.setFloat(2, p.getPrice());
            stmt.setString(3, p.getName());
            stmt.setString(4, p.getDescription());
            stmt.setInt(5, p.getQuant_in_stock());
            stmt.setBoolean(6, p.isIs_visible());
            stmt.setInt(7, p.getId_category());
            stmt.setString(8, p.getUsername_seller());
            stmt.setInt(9, p.getId_delivery_type());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updateProductReview(product_review p) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call update_product(?,?,?,?)}");
            stmt.setInt(1, p.getId());
            stmt.setFloat(2, p.getScore());
            stmt.setString(3, p.getComment());
            stmt.setString(4, p.getUsername_writer());
            stmt.setInt(5, p.getIdProduct());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updateReviewType(review_type p) {
        
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call update_reviewType(?)}");
            stmt.setInt(1, p.getId());
            stmt.setString(2, p.getName());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updateShoppingCart(shopping_cart p) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call update_shoppingCart(?,?,?)}");
            stmt.setString(1, p.getUsername());
            stmt.setInt(2, p.getId_product());
            stmt.setInt(3, p.getQuantity());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updateUser(user u) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call update_user(?,?,?)}");
            stmt.setString(1, u.getUsername());
            stmt.setString(2, u.getPassword());
            stmt.setInt(3, u.getId_user_type());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updateUserReview(user_review u) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call update_userReview(?,?,?,?,?,?)}");
            stmt.setInt(1, u.getId());
            stmt.setFloat(2, u.getScore());
            stmt.setString(3, u.getComment());
            stmt.setInt(4, u.getId_type_review());
            stmt.setString(5, u.getUsername_writer());
            stmt.setString(6, u.getUsername_receiver());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updateUserType(user_type ut) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call update_userType(?,?)}");
            stmt.setInt(1, ut.getId());
            stmt.setString(2, ut.getName());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updateWishList(wish_list w) {
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call update_wishList(?,?,?)}");
            stmt.setString(1, w.getUsername());
            stmt.setInt(2, w.getId_product());
            stmt.setInt(3, w.getQuantity());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updateCity(city c) {
        try{
            con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call `update_city`(?,?,?)}");
            stmt.setInt(1, c.getId());
            stmt.setString(2, c.getName());
            stmt.setInt(3, c.getId_state());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updateCountry(country p) {
        try{
            con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call `update_country`(?,?)}");
            stmt.setInt(1, p.getId());
            stmt.setString(2, p.getName());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updateDistrict(district p) {
        try{
            con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call `update_district`(?,?,?)}");
            stmt.setInt(1, p.getId());
            stmt.setString(2, p.getName());
            stmt.setInt(3, p.getId_city());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updateGender(gender p) {
        try{
            con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call `update_gender`(?)}");
            stmt.setInt(1, p.getId());
            stmt.setString(2, p.getName());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updateNationality(nationality p) {
        try{
            con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call `update_nationality`(?)}");
            stmt.setInt(1, p.getId());
            stmt.setString(2, p.getName());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updatePerson(person p) {
        try{
            con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call `update_person`(?,?,?,?,?,?,?,?,?,?,?)}");
            stmt.setString(1, p.getId());
            stmt.setString(2, p.getFirst_name());
            stmt.setString(3, p.getMiddle_name());
            stmt.setString(4, p.getLast_name());
            stmt.setString(5, p.getEmail());
            stmt.setString(6, p.getPhone_number());
            stmt.setDate(7, p.getBirthday());
            stmt.setString(8, p.getPicture_path());
            stmt.setInt(9, p.getId_gender());
            stmt.setInt(10, p.getId_district());
            stmt.setString(11, p.getUsername());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void updateState(state p) {
        try{
            con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call `update_state`(?,?,?)}");
            stmt.setInt(1, p.getId());
            stmt.setString(2, p.getName());
            stmt.setInt(3, p.getId_country());
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    
    
    //Removes
    public void removeWithId(int pId, String function,boolean inApp) {
        try{
            if(inApp)
                con=  DriverManager.getConnection(url_app, user_app, pass_app);
            else
                con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call "+ function +" (?)}");
            stmt.setInt(1, pId);
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void removeWithUsername(String pUsername, String function, boolean inApp) {        
        try{
            if(inApp)
                con=  DriverManager.getConnection(url_app, user_app, pass_app);
            else
                con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call "+ function +" (?)}");
            stmt.setString(1, pUsername);
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    public void removeWithStringandInt(String pUsername, int pId, String function, boolean inApp) {        
        try{
            if(inApp)
                con=  DriverManager.getConnection(url_app, user_app, pass_app);
            else
                con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call "+ function +" (?,?)}");
            stmt.setString(1, pUsername);
            stmt.setInt(2, pId);
            stmt.execute();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
    }
    
    //Getters
    
    public String getStringWithInt(int pId, String function,boolean inApp) {
        String result = null;
        try{
            if(inApp)
                con=  DriverManager.getConnection(url_app, user_app, pass_app);
            else
                con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{ ? = call "+ function +" (?)}");
            stmt.registerOutParameter(1, Types.VARCHAR);
            stmt.setInt(2, pId);
            stmt.execute();
            result = stmt.getString(1);
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
        return result;
    }
    
    public int getIntWithId(int pId, String function,boolean inApp) {
        int result = -1;
        try{
            if(inApp)
                con=  DriverManager.getConnection(url_app, user_app, pass_app);
            else
                con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{ ? = call "+ function +" (?)}");
            stmt.registerOutParameter(1, Types.INTEGER);
            stmt.setInt(2, pId);
            stmt.execute();
            result = stmt.getInt(1);
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
        return result;
    }
    
    public float getFloatWithId(int pId, String function,boolean inApp) {
        float result = -1;
        try{
            if(inApp)
                con=  DriverManager.getConnection(url_app, user_app, pass_app);
            else
                con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{ ? = call "+ function +" (?)}");
            stmt.registerOutParameter(1, Types.DECIMAL);
            stmt.setInt(2, pId);
            stmt.execute();
            result = stmt.getInt(1);
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
        return result;
    }
    
    public Date getDateWithId(int pId, String function,boolean inApp) {
        java.sql.Date result = null;
        try{
            if(inApp)
                con=  DriverManager.getConnection(url_app, user_app, pass_app);
            else
                con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{ ? = call "+ function +" (?)}");
            stmt.registerOutParameter(1, Types.DATE);
            stmt.setInt(2, pId);
            stmt.execute();
            result = stmt.getDate(1);
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
        return result;
    }
    
    public String getStringWithString(String p, String function,boolean inApp) {
        con=null;
        String result = null;
        try{
            if(inApp)
                con=  DriverManager.getConnection(url_app, user_app, pass_app);
            else
                con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{ ? = call "+ function +" (?)}");
            stmt.registerOutParameter(1, Types.VARCHAR);
            stmt.setString(2, p);
            stmt.execute();
            result = stmt.getString(1);
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
        return result;
    }
    
    public int getIntWithString(String p, String function,boolean inApp) {
        int result = -1;
        try{
            if(inApp)
                con=  DriverManager.getConnection(url_app, user_app, pass_app);
            else
                con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{ ? = call "+ function +" (?)}");
            stmt.registerOutParameter(1, Types.INTEGER);
            stmt.setString(2, p);
            stmt.execute();
            result = stmt.getInt(1);
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
        return result;
    }
    
    public float getFloatWithString(String p, String function,boolean inApp) {
        float result = -1;
        try{
            if(inApp)
                con=  DriverManager.getConnection(url_app, user_app, pass_app);
            else
                con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{ ? = call "+ function +" (?)}");
            stmt.registerOutParameter(1, Types.DECIMAL);
            stmt.setString(2, p);
            stmt.execute();
            result = stmt.getInt(1);
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
        return result;
    }
    
    public Date getDateWithString(String p, String function,boolean inApp) {
        java.sql.Date result = null;
        try{
            if(inApp)
                con=  DriverManager.getConnection(url_app, user_app, pass_app);
            else
                con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{ ? = call "+ function +" (?)}");
            stmt.registerOutParameter(1, Types.DATE);
            stmt.setString(2, p);
            stmt.execute();
            result = stmt.getDate(1);
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
        return result;
    }
    
    // Funciones varias

    public boolean checkLogin(String username, String password) {
        boolean result = false;
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{ ? = call check_login (?,?)}");
            stmt.registerOutParameter(1, Types.BOOLEAN);
            stmt.setString(2, username);
            stmt.setString(3, password);
            stmt.execute();
            result = stmt.getBoolean(1);
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
        return result;
    }
    
    public ResultSet query(String function, boolean inApp){
        ResultSet result = null;
        try{
            if(inApp)
                con=  DriverManager.getConnection(url_app, user_app, pass_app);
            else
                con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call "+ function +" ()}");
            result = stmt.executeQuery();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
        return result;
    }
     
    public ResultSet queryWithInt(int p, String function, boolean inApp){
        ResultSet result = null;
        try{
            if(inApp)
                con=  DriverManager.getConnection(url_app, user_app, pass_app);
            else
                con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call "+ function +" (?)}");
            stmt.setInt(1, p);
            result = stmt.executeQuery();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
        return result;
    }
    
    public ResultSet queryWithStringAndInt(String p1, int p2, String function, boolean inApp){
        ResultSet result = null;
        try{
            if(inApp)
                con=  DriverManager.getConnection(url_app, user_app, pass_app);
            else
                con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call "+ function +" (?,?)}");
            stmt.setString(1, p1);
            stmt.setInt(2, p2);
            result = stmt.executeQuery();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
        return result;
    }
    
    public ResultSet queryWithString(String p, String function, boolean inApp){
        ResultSet result = null;
        try{
            if(inApp)
                con=  DriverManager.getConnection(url_app, user_app, pass_app);
            else
                con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call "+ function +" (?)}");
            stmt.setString(1, p);
            result = stmt.executeQuery();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
        return result;
    }
    
    public ResultSet queryWithStrings(String p1, String p2, String function, boolean inApp){
        ResultSet result = null;
        try{
            if(inApp)
                con=  DriverManager.getConnection(url_app, user_app, pass_app);
            else
                con=  DriverManager.getConnection(url_person, user_person, pass_person);
            CallableStatement stmt = con.prepareCall("{call "+ function +" (?,?)}");
            stmt.setString(1, p1);
            stmt.setString(2, p2);
            result = stmt.executeQuery();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
        return result;
    }
    
    public ResultSet topExpensivesOfCategory(int rowsq, int pIdCategory){
        ResultSet result = null;
        try{
            con=  DriverManager.getConnection(url_app, user_app, pass_app);
            CallableStatement stmt = con.prepareCall("{call `top_expensives_of_category` (?,?)}");
            stmt.setInt(1, rowsq);
            stmt.setInt(2, pIdCategory);
            result = stmt.executeQuery();
        }
        catch (Exception e){
            System.out.println("Error de conexion: " + e);
        }
        return result;
    } 
}
