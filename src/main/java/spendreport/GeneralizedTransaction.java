package spendreport;

import org.apache.flink.walkthrough.common.entity.Transaction;

public class GeneralizedTransaction {
    private long accountId;
    private long timestamp;
    private double amountLowerBound;
    private double amountUpperBound;

    public GeneralizedTransaction(Transaction t, double lowerBound, double upperBound){
        this.accountId = t.getAccountId();
        this.timestamp = t.getTimestamp();
        this.amountLowerBound = lowerBound;
        this.amountUpperBound = upperBound;
    }

    public String toString() {
        return "Transaction{accountId=" + this.accountId + ", timestamp=" + this.timestamp + ", amount=[" + this.amountLowerBound + ", " + this.amountUpperBound + '}';
    }
}
