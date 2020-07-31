/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.tpcc.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConfig;
import com.oltpbenchmark.benchmarks.tpcc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpcc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpcc.TPCCWorker;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;

import com.oltpbenchmark.types.DatabaseType;

public class NewOrder extends TPCCProcedure {

    private static final Logger LOG = Logger.getLogger(NewOrder.class);

    public final SQLStmt stmtGetCustSQL = new SQLStmt(
        "SELECT C_DISCOUNT, C_LAST, C_CREDIT" +
          "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
          " WHERE C_W_ID = ? " +
          "   AND C_D_ID = ? " +
          "   AND C_ID = ?");

    public final SQLStmt stmtGetWhseSQL = new SQLStmt(
        "SELECT W_TAX " +
        "  FROM " + TPCCConstants.TABLENAME_WAREHOUSE +
        " WHERE W_ID = ?");

    public final SQLStmt stmtGetDistSQL = new SQLStmt(
        "SELECT D_NEXT_O_ID, D_TAX " +
          "  FROM " + TPCCConstants.TABLENAME_DISTRICT +
          " WHERE D_W_ID = ? AND D_ID = ? FOR UPDATE");

  public final SQLStmt  stmtInsertNewOrderSQL = new SQLStmt(
          "INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER +
          " (NO_O_ID, NO_D_ID, NO_W_ID) " +
            " VALUES ( ?, ?, ?)");

  public final SQLStmt  stmtUpdateDistSQL = new SQLStmt(
          "UPDATE " + TPCCConstants.TABLENAME_DISTRICT +
          "   SET D_NEXT_O_ID = D_NEXT_O_ID + 1 " +
            " WHERE D_W_ID = ? " +
          "   AND D_ID = ?");

  public final SQLStmt  stmtInsertOOrderSQL = new SQLStmt(
          "INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER +
          " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?)");

  public SQLStmt[] stmtGetItemSQL;
  public SQLStmt[] stmtGetStockSQL;
  public SQLStmt[] stmtUpdateStockSQL;
  public SQLStmt[] stmtInsertOrderLineSQL;


  // NewOrder Txn
  private PreparedStatement stmtGetCust = null;
  private PreparedStatement stmtGetWhse = null;
  private PreparedStatement stmtGetDist = null;
  private PreparedStatement stmtInsertNewOrder = null;
  private PreparedStatement stmtUpdateDist = null;
  private PreparedStatement stmtInsertOOrder = null;
  private PreparedStatement stmtUpdateStock = null;
  private PreparedStatement stmtInsertOrderLine = null;
  private PreparedStatement stmtGetItem = null;
  private PreparedStatement stmtGetStock = null;

  public NewOrder() {
    stmtGetItemSQL = new SQLStmt[15];
    stmtGetStockSQL = new SQLStmt[15];
    stmtUpdateStockSQL = new SQLStmt[15];
    stmtInsertOrderLineSQL = new SQLStmt[11];

    // We create 15 statements for selecting rows from the `ITEM` table with varying number of ITEM
    // ids.  Each string looks like:
    // SELECT I_ID, I_PRICE, I_NAME , I_DATA
    // FROM ITEM
    // WHERE I_ID in (?, ? ..);
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("SELECT I_ID, I_PRICE, I_NAME , I_DATA FROM %s WHERE I_ID in (",
                            TPCCConstants.TABLENAME_ITEM));
    for (int ii = 1; ii <= 15; ++ii) {
      if (ii == 1) {
        sb.append("?");
      } else {
        sb.append(",?");
      }
      stmtGetItemSQL[ii - 1] = new SQLStmt(sb.toString() + ")");
    }

    // We create 15 statements for selecting rows from the `STOCK` table with varying number of
    // ITEM ids and a fixed WAREHOUSE id. Each string looks like:
    // SELECT I_I, I_NAME , I_DATA
    // FROM STOCK
    // WHERE S_W_ID = ? AND S_I_ID in (?, ? ..);
    sb = new StringBuilder();
    sb.append(
      String.format("SELECT S_W_ID, S_I_ID, S_QUANTITY, S_DATA, S_YTD, S_REMOTE_CNT, S_DIST_01, " +
                    "S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, " +
                    "S_DIST_09, S_DIST_10 FROM %s WHERE S_W_ID = ? AND S_I_ID in (",
                    TPCCConstants.TABLENAME_STOCK));
    for (int ii = 1; ii <= 15; ++ii) {
      if (ii == 1) {
        sb.append("?");
      } else {
        sb.append(",?");
      }
      stmtGetStockSQL[ii - 1] = new SQLStmt(sb.toString() + ") FOR UPDATE");
    }

    // We create 15 statements to update the rows in `STOCK` table. Each string looks like:
    // CALL updatestock[0-9]*(?, ? ...)
    sb = new StringBuilder();
    sb.append("?");
    for (int ii = 1; ii <= 15; ++ii) {
      sb.append(", ?, ?, ?, ?");
      stmtUpdateStockSQL[ii - 1] = new SQLStmt(String.format("CALL updatestock%d(%s)",
                                                             ii, sb.toString()));
    }

    // We create 11 statements that insert into `ORDERLINE`. Each string looks like:
    // INSERT INTO ORDERLINE
    // (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO)
    // VALUES (?,?,?,?,?,?,?,?,?), (?,?,?,?,?,?,?,?,?) ..
    sb = new StringBuilder();
    sb.append(String.format("INSERT INTO %s (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, " +
                            "OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES",
                            TPCCConstants.TABLENAME_ORDERLINE));
    for (int ii = 1; ii <= 15; ++ii) {
      if (ii == 1) {
        sb.append("(?,?,?,?,?,?,?,?,?)");
      } else {
        sb.append(", (?,?,?,?,?,?,?,?,?)");
      }
      if (ii >= 5) {
        stmtInsertOrderLineSQL[ii - 5] = new SQLStmt(sb.toString());
      }
    }
  }

  public ResultSet run(Connection conn, Random gen,
      int terminalWarehouseID, int numWarehouses,
      int terminalDistrictLowerID, int terminalDistrictUpperID,
      TPCCWorker w) throws SQLException {

    int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID,terminalDistrictUpperID, gen);
    int customerID = TPCCUtil.getCustomerID(gen);
    int numItems = (int) TPCCUtil.randomNumber(5, 15, gen);
    int[] itemIDs = new int[numItems];
    int[] supplierWarehouseIDs = new int[numItems];
    int[] orderQuantities = new int[numItems];
    int allLocal = 1;
    for (int i = 0; i < numItems; i++) {
      itemIDs[i] = TPCCUtil.getItemID(gen);
      if (TPCCUtil.randomNumber(1, 100, gen) > 1) {
        supplierWarehouseIDs[i] = terminalWarehouseID;
      } else {
        do {
          supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1,
              numWarehouses, gen);
        } while (supplierWarehouseIDs[i] == terminalWarehouseID
            && numWarehouses > 1);
        allLocal = 0;
      }
      orderQuantities[i] = TPCCUtil.randomNumber(1, 10, gen);
    }

    // we need to cause 1% of the new orders to be rolled back.
    if (TPCCUtil.randomNumber(1, 100, gen) == 1)
      itemIDs[numItems - 1] = TPCCConfig.INVALID_ITEM_ID;

    //initializing all prepared statements
    stmtGetCust=this.getPreparedStatement(conn, stmtGetCustSQL);
    stmtGetWhse=this.getPreparedStatement(conn, stmtGetWhseSQL);
    stmtGetDist=this.getPreparedStatement(conn, stmtGetDistSQL);
    stmtInsertNewOrder=this.getPreparedStatement(conn, stmtInsertNewOrderSQL);
    stmtUpdateDist =this.getPreparedStatement(conn, stmtUpdateDistSQL);
    stmtInsertOOrder =this.getPreparedStatement(conn, stmtInsertOOrderSQL);
    stmtInsertOrderLine =this.getPreparedStatement(conn, stmtInsertOrderLineSQL[numItems - 5]);

    newOrderTransaction(terminalWarehouseID, districtID,
            customerID, numItems, allLocal, itemIDs,
            supplierWarehouseIDs, orderQuantities, conn, w);
    return null;

    }

  private void newOrderTransaction(int w_id, int d_id, int c_id,
      int o_ol_cnt, int o_all_local, int[] itemIDs,
      int[] supplierWarehouseIDs, int[] orderQuantities, Connection conn, TPCCWorker w)
      throws SQLException {
    float c_discount, w_tax, d_tax = 0, i_price;
    int d_next_o_id, o_id = -1, s_quantity;
    String c_last = null, c_credit = null, i_name, i_data, s_data;
    String ol_dist_info = null;
    float[] itemPrices = new float[o_ol_cnt];
    float[] orderLineAmounts = new float[o_ol_cnt];
    String[] itemNames = new String[o_ol_cnt];
    int[] stockQuantities = new int[o_ol_cnt];
    char[] brandGeneric = new char[o_ol_cnt];
    int ol_supply_w_id, ol_i_id, ol_quantity;
    int s_remote_cnt_increment;
    float ol_amount, total_amount = 0;

    try {
      stmtGetCust.setInt(1, w_id);
      stmtGetCust.setInt(2, d_id);
      stmtGetCust.setInt(3, c_id);
      ResultSet rs = stmtGetCust.executeQuery();
      if (!rs.next())
        throw new RuntimeException("C_D_ID=" + d_id
            + " C_ID=" + c_id + " not found!");
      c_discount = rs.getFloat("C_DISCOUNT");
      c_last = rs.getString("C_LAST");
      c_credit = rs.getString("C_CREDIT");
      rs.close();
      rs = null;

      stmtGetWhse.setInt(1, w_id);
      rs = stmtGetWhse.executeQuery();
      if (!rs.next())
        throw new RuntimeException("W_ID=" + w_id + " not found!");
      w_tax = rs.getFloat("W_TAX");
      rs.close();
      rs = null;

      stmtGetDist.setInt(1, w_id);
      stmtGetDist.setInt(2, d_id);
      rs = stmtGetDist.executeQuery();
      if (!rs.next()) {
        throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id
            + " not found!");
      }
      d_next_o_id = rs.getInt("D_NEXT_O_ID");
      d_tax = rs.getFloat("D_TAX");
      rs.close();
      rs = null;

      //woonhak, need to change order because of foreign key constraints
      //update next_order_id first, but it might doesn't matter
      stmtUpdateDist.setInt(1, w_id);
      stmtUpdateDist.setInt(2, d_id);
      int result = stmtUpdateDist.executeUpdate();
      if (result == 0)
        throw new RuntimeException(
            "Error!! Cannot update next_order_id on district for D_ID="
                + d_id + " D_W_ID=" + w_id);

      o_id = d_next_o_id;

      // woonhak, need to change order, because of foreign key constraints
      //[[insert ooder first
      stmtInsertOOrder.setInt(1, o_id);
      stmtInsertOOrder.setInt(2, d_id);
      stmtInsertOOrder.setInt(3, w_id);
      stmtInsertOOrder.setInt(4, c_id);
      stmtInsertOOrder.setTimestamp(5, w.getBenchmarkModule().getTimestamp(System.currentTimeMillis()));
      stmtInsertOOrder.setInt(6, o_ol_cnt);
      stmtInsertOOrder.setInt(7, o_all_local);
      stmtInsertOOrder.executeUpdate();
      //insert ooder first]]
      /*TODO: add error checking */

      stmtInsertNewOrder.setInt(1, o_id);
      stmtInsertNewOrder.setInt(2, d_id);
      stmtInsertNewOrder.setInt(3, w_id);
      stmtInsertNewOrder.executeUpdate();
      /*TODO: add error checking */

      float[] i_price_arr = new float[o_ol_cnt];
      String[] i_name_arr = new String[o_ol_cnt];
      String[] i_data_arr = new String[o_ol_cnt];

      int[] s_qty_arr = new int[o_ol_cnt];
      String[] s_data_arr = new String[o_ol_cnt];
      String[] ol_dist_info_arr = new String[o_ol_cnt];
      int[] ytd_arr = new int[o_ol_cnt];
      int[] remote_cnt_arr = new int[o_ol_cnt];

      getItemsAndStock(o_ol_cnt, d_id,
                       itemIDs, supplierWarehouseIDs, orderQuantities,conn,
                       i_price_arr, i_name_arr, i_data_arr,
                       s_qty_arr, s_data_arr, ol_dist_info_arr,
                       ytd_arr, remote_cnt_arr);

      updateStock(o_ol_cnt, w_id, itemIDs, supplierWarehouseIDs,
                  orderQuantities, s_qty_arr, ytd_arr, remote_cnt_arr, conn);

      total_amount = insertOrderLines(o_id, w_id, d_id, o_ol_cnt, itemIDs,
                                      supplierWarehouseIDs, orderQuantities,
                                      i_price_arr, i_data_arr, s_data_arr,
                                      ol_dist_info_arr, orderLineAmounts,
                                      brandGeneric);
      total_amount *= (1 + w_tax + d_tax) * (1 - c_discount);
    } catch(UserAbortException userEx) {
        LOG.debug("Caught an expected error in New Order");
        throw userEx;
    } finally {
        if (stmtInsertOrderLine != null)
            stmtInsertOrderLine.clearBatch();
        if (stmtUpdateStock != null)
            stmtUpdateStock.clearBatch();
    }
  }

  void getItemsAndStock(int o_ol_cnt, int d_id,
                        int[] itemIDs, int[] supplierWarehouseIDs, int[] orderQuantities, Connection conn,
                        float[] i_price_arr, String[] i_name_arr, String[] i_data_arr,
                        int[] s_qty_arr, String[] s_data_arr, String[] ol_dist_info_arr,
                        int[] ytd_arr, int[] remote_cnt_arr) throws  SQLException {
    Map<Integer, HashSet<Integer>> input = new HashMap<>();
    for (int ii = 0; ii < o_ol_cnt; ++ii) {
      int itemId = itemIDs[ii];
      int supplierWh = supplierWarehouseIDs[ii];
      if (!input.containsKey(supplierWh)) {
        input.put(supplierWh, new HashSet<>());
      }
      input.get(supplierWh).add(itemId);
    }

    for (Map.Entry<Integer, HashSet<Integer>> entry : input.entrySet()) {
      stmtGetItem = this.getPreparedStatement(conn, stmtGetItemSQL[entry.getValue().size() - 1]);
      int k = 1;
      for (int val : entry.getValue()) {
        stmtGetItem.setInt(k++, val);
      }
      ResultSet rs1 = stmtGetItem.executeQuery();

      stmtGetStock = this.getPreparedStatement(conn, stmtGetStockSQL[entry.getValue().size() - 1]);
      k = 1;
      stmtGetStock.setInt(k++, entry.getKey());
      for (int val : entry.getValue()) {
        stmtGetStock.setInt(k++, val);
      }
      ResultSet rs2 = stmtGetStock.executeQuery();

      Map<Integer, Integer> m = new HashMap<>();
      for (int ii = 0; ii < itemIDs.length; ++ii) {
        int expected = itemIDs[ii];
        if (m.containsKey(expected)) {
          continue;
                }
        if (!rs1.next()) {
          throw new UserAbortException("EXPECTED new order rollback: I_ID=" +
                                       TPCCConfig.INVALID_ITEM_ID + "not found");
        }
        if (!rs2.next()) {
          throw new UserAbortException("EXPECTED new order rollback: I_ID=" +
                                       TPCCConfig.INVALID_ITEM_ID + "not found");
        }

        int itemId = rs1.getInt("I_ID");
        assert (itemId == expected);
        itemId = rs2.getInt("S_I_ID");
        assert (itemId == expected);

        float price = rs1.getFloat("I_PRICE");
        String name = rs1.getString("I_NAME");
        String data = rs1.getString("I_DATA");

        int s_quantity = rs2.getInt("S_QUANTITY");
        String s_data = rs2.getString("S_DATA");
        String ol_dist_info = getDistInfo(rs2, d_id);
        int wHId = rs2.getInt("S_W_ID");

        int ytd = rs2.getInt("S_YTD");
        int remote_cnt = rs2.getInt("S_REMOTE_CNT");

        storeInfo(itemIDs, supplierWarehouseIDs, orderQuantities,
            itemId, wHId,
            price, name, data, s_quantity, s_data, ol_dist_info,
            ytd, remote_cnt,
            i_price_arr, i_name_arr, i_data_arr,
            s_qty_arr, s_data_arr, ol_dist_info_arr,
            ytd_arr, remote_cnt_arr);
      }
      rs1.close();
      rs2.close();
    }
  }

  String getDistInfo(ResultSet rs, int d_id) throws SQLException {
    switch (d_id) {
      case 1:
        return rs.getString("S_DIST_01");
      case 2:
        return rs.getString("S_DIST_02");
      case 3:
        return rs.getString("S_DIST_03");
      case 4:
        return rs.getString("S_DIST_04");
      case 5:
        return rs.getString("S_DIST_05");
      case 6:
        return rs.getString("S_DIST_06");
      case 7:
        return rs.getString("S_DIST_07");
      case 8:
        return rs.getString("S_DIST_08");
      case 9:
        return rs.getString("S_DIST_09");
      case 10:
        return rs.getString("S_DIST_10");
    }
    return "";
  }

  void storeInfo(int[] itemIDs, int[] supplierWhs, int[] orderQuantities,
                 int itemId, int supplierWh,
                 float price, String name, String i_data,
                 int qty, String s_data, String dist_info,
                 int ytd, int remote_cnt,
                 float[] i_price_arr, String[] i_name_arr, String[] i_data_arr,
                 int[] qty_arr, String[] data_arr, String[] dist_info_arr,
                 int[] ytd_arr, int[] remote_cnt_arr) {
    for (int ii = 0; ii < itemIDs.length; ++ii) {
      if (itemId == itemIDs[ii] && supplierWh == supplierWhs[ii]) {
        i_price_arr[ii] = price;
        i_name_arr[ii] = name;
        i_data_arr[ii] = i_data;

        qty_arr[ii] = qty;
        data_arr[ii] = s_data;
        dist_info_arr[ii]= dist_info;
        ytd_arr[ii] = ytd;
        remote_cnt_arr[ii] = remote_cnt;

        // Note that the same item could be present in the itemID multiple times. So adjust the new
        // quantity for the next time accordingly.
        if (qty - orderQuantities[ii] >= 10) {
          qty -= orderQuantities[ii];
        } else {
          qty += (91 -orderQuantities[ii]);
        }
      }
    }
  }

  void updateStock(int o_ol_cnt, int w_id,
                   int[] itemIDs, int[] supplierWarehouseIDs, int[] orderQuantities,
                   int[] s_qty_arr, int[] ytd_arr, int[] remote_cnt_arr,
                   Connection conn) throws  SQLException {

    Map<Integer, HashSet<Integer>> input = new HashMap<>();
    for (int ii = 0; ii < o_ol_cnt; ++ii) {
      int itemId = itemIDs[ii];
      int supplierWh = supplierWarehouseIDs[ii];
      if (!input.containsKey(supplierWh)) {
        input.put(supplierWh, new HashSet<>());
      }
      input.get(supplierWh).add(itemId);
    }

    for (Map.Entry<Integer, HashSet<Integer>> entry : input.entrySet()) {
      int whId = entry.getKey();
      int numEntries = entry.getValue().size();
      stmtUpdateStock = this.getPreparedStatement(conn, stmtUpdateStockSQL[numEntries - 1]);

      int ii = 1;
      stmtUpdateStock.setInt(ii++, whId);
      for (int itemId : entry.getValue()) {
        int index = getIndex(itemId, itemIDs);
        int s_quantity = s_qty_arr[index];
        int ol_quantity = getOrderQts(itemId, itemIDs, orderQuantities);

        if (s_quantity - ol_quantity >= 10) {
          s_quantity -= ol_quantity;
        } else {
          s_quantity += -ol_quantity + 91;
        }
        int s_remote_cnt_increment;
        if (whId == w_id) {
          s_remote_cnt_increment = 0;
        } else {
          s_remote_cnt_increment = 1;
        }

        stmtUpdateStock.setInt(ii++, itemId);
        stmtUpdateStock.setInt(ii++, s_quantity);
        stmtUpdateStock.setInt(ii++, ytd_arr[index] + ol_quantity);
        stmtUpdateStock.setInt(ii++, remote_cnt_arr[index] + s_remote_cnt_increment);
      }
      stmtUpdateStock.execute();
    }
  }

  int getIndex(int itemId, int[] itemIDs) {
    int idx = -1;
    for (int ii = 0; ii < itemIDs.length; ++ii) {
      int v = itemIDs[ii];
      if (v == itemId) {
        idx = ii;
      }
    }
    return idx;
  }

  int getOrderQts(int itemID, int[] itemIDs, int[] orderQts) {
    int out = 0;
    for (int ii = 0; ii < itemIDs.length; ++ii) {
      int v = itemIDs[ii];
      if (itemID == v) {
        out += orderQts[ii];
      }
    }
    return out;
  }

  int insertOrderLines(int o_id, int w_id, int d_id,
                       int o_ol_cnt, int[] itemIDs,
                       int[] supplierWarehouseIDs, int[] orderQuantities,
                       float[] i_price_arr, String[] i_data_arr,
                       String[] s_data_arr, String[] ol_dist_info_arr,
                       float[] orderLineAmounts, char[] brandGeneric) throws SQLException {

    int total_amount = 0;
    int k = 1;
    for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
      int ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
      int ol_i_id = itemIDs[ol_number - 1];
      int ol_quantity = orderQuantities[ol_number - 1];

      float i_price = i_price_arr[ol_number - 1];
      String i_data = i_data_arr[ol_number - 1];

      String s_data = s_data_arr[ol_number - 1];
      String ol_dist_info = ol_dist_info_arr[ol_number - 1];

      float ol_amount = ol_quantity * i_price;
      orderLineAmounts[ol_number - 1] = ol_amount;
      total_amount += ol_amount;

      if (i_data.indexOf("ORIGINAL") != -1
          && s_data.indexOf("ORIGINAL") != -1) {
        brandGeneric[ol_number - 1] = 'B';
      } else {
        brandGeneric[ol_number - 1] = 'G';
      }

      stmtInsertOrderLine.setInt(k++, o_id);
      stmtInsertOrderLine.setInt(k++, d_id);
      stmtInsertOrderLine.setInt(k++, w_id);
      stmtInsertOrderLine.setInt(k++, ol_number);
      stmtInsertOrderLine.setInt(k++, ol_i_id);
      stmtInsertOrderLine.setInt(k++, ol_supply_w_id);
      stmtInsertOrderLine.setInt(k++, ol_quantity);
      stmtInsertOrderLine.setDouble(k++, ol_amount);
      stmtInsertOrderLine.setString(k++, ol_dist_info);
    }
    stmtInsertOrderLine.execute();
    return total_amount;
  }
}
