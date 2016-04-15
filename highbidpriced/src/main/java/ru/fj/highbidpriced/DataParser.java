package ru.fj.highbidpriced;

class DataParser {

    private static final int USER_AGENT_IND = 4;
    private static final int BIDDING_PRICE_IND = 19;
    private static final int CITY_IND = 7;
    private static final int REGION_IND = 6;

    private OsDataParser osDataParser = new OsDataParser();

    CompositeKey getData(String line) {
        CompositeKey result = new CompositeKey();
        String[] split = line.split("\\t", 21);
        result.region = split[REGION_IND];
        result.city = split[CITY_IND];
        result.operationSystem = osDataParser.getOperationSystem(split[USER_AGENT_IND]);
        try {
            result.biddingPrice = Integer.valueOf(split[BIDDING_PRICE_IND]);
        } catch (NumberFormatException e) {
            result.biddingPrice = 0;
        }
        return result;
    }

}
