# from airflow.plugins_manager import AirflowPlugin
#
# from plugins.operators.google_analytics_to_s3_operator import GoogleAnalyticsSourceToS3Operator, \
#     GoogleAnalyticsChannelGroupingToS3Operator, GoogleAnalyticsAdwordsToS3Operator, \
#     GoogleAnalyticsDemographicsToS3Operator, \
#     GoogleAnalyticsLandingPageToS3Operator, GoogleAnalyticsEventsToS3Operator, GoogleAnalyticsPagesToS3Operator
# from plugins.operators.google_search_console_to_s3_operator import GoogleSearchConsoleToS3Operator
# from operators.fbads_to_s3_operator import FacebookToS3Operator, FacebookAdsToS3Operator, \
#     FacebookAdsetsToS3Operator, FacebookCampaignsToS3Operator, FacebookAdInsightsToS3Operator, \
#     FacebookAdsetInsightsToS3Operator, FacebookCampaignInsightsToS3Operator, FacebookAdCreativesToS3Operator, \
#     FacebookAccountsToS3Operator
# from plugins.operators.webpage_test_to_s3_operator import WptesterToS3Operator
# from plugins.operators.shiprocket_to_s3_operator import ShiprocketToS3Operator, ShiprocketShipmentsToS3Operator
# from plugins.operators.s3_to_redshift_operator import S3ToRedshiftOperator
#
# from plugins.operators.amz_to_s3_operator import *
# from plugins.operators.google_ads_to_s3_operator import *
# from plugins.operators.magento2_to_s3_operator import *
# from plugins.operators.create_rule_insights_operator import GenerateRuleInsightsOperator
# from plugins.operators.rule_insights_engine_operator import RuleEngine
#
# from plugins.hooks.google_ads_hook import GoogleAdsHook
# from plugins.hooks.magento2_hook import Magento2Hook
# from plugins.hooks.facebook_ads_hook import FacebookAdsHook
# from plugins.hooks.webpage_test_hook import WptestHook
# from plugins.hooks.get_rules_sheet import Sheet
# from plugins.hooks.polaris_interface import PolarisInterface
#
# from plugins.macros.redshift_auth import redshift_auth
#
#
# class RulesInsightsPlugin(AirflowPlugin):
#     name = "RulesInsightsPlugin"
#     hooks = [Sheet,
#              PolarisInterface]
#     operators = [GenerateRuleInsightsOperator,
#                  RuleEngine]
#
#
# class FacebookAdsPlugin(AirflowPlugin):
#     name = "FacebookAdsPlugin"
#     hooks = [FacebookAdsHook]
#     operators = [FacebookToS3Operator,
#                  FacebookAdsToS3Operator,
#                  FacebookAdsetsToS3Operator,
#                  FacebookCampaignsToS3Operator,
#                  FacebookAdInsightsToS3Operator,
#                  FacebookAdsetInsightsToS3Operator,
#                  FacebookCampaignInsightsToS3Operator,
#                  FacebookAccountsToS3Operator,
#                  FacebookAdCreativesToS3Operator]
#
#
# class GoogleAdsPlugin(AirflowPlugin):
#     name = 'GoogleAdsPlugin'
#     hooks = [GoogleAdsHook]
#     operators = [
#         GoogleToS3Operator,
#         GoogleAccountsToS3Operator,
#         GoogleCampaignsToS3Operator,
#         GoogleAdgroupsToS3Operator,
#         GoogleCampaignPerformanceToS3Operator,
#         GoogleAdgroupPerformanceToS3Operator,
#         GoogleAdPerformanceToS3Operator,
#         GoogleKeywordPerformanceToS3Operator,
#         GoogleAgePerformanceToS3Operator,
#         GoogleGenderPerformanceToS3Operator,
#         GoogleGeoPerformanceToS3Operator
#     ]
#     executors = []
#     macros = []
#     admin_views = []
#     flask_blueprints = []
#     menu_links = []
#
#
# class GoogleAnalyticsPlugin(AirflowPlugin):
#     name = "google_analytics_plugin"
#     hooks = []
#     operators = [GoogleAnalyticsSourceToS3Operator,
#                  GoogleAnalyticsChannelGroupingToS3Operator,
#                  GoogleAnalyticsAdwordsToS3Operator,
#                  GoogleAnalyticsDemographicsToS3Operator,
#                  GoogleAnalyticsLandingPageToS3Operator,
#                  GoogleAnalyticsEventsToS3Operator,
#                  GoogleAnalyticsPagesToS3Operator]
#
#
# class MagentoPlugin(AirflowPlugin):
#     name = 'MagentoPlugin'
#     hooks = [Magento2Hook]
#     operators = [
#         MagentoToS3Operator,
#         MagentoOrdersToS3Operator,
#         MagentoProductsToS3Operator,
#         MagentoAbandonedCartsToS3Operator,
#         MagentoCategoriesToS3Operator,
#         MagentoCustomersToS3Operator
#     ]
#     executors = []
#     macros = []
#     admin_views = []
#     flask_blueprints = []
#     menu_links = []
#
#
# class GoogleSearchConsolePlugin(AirflowPlugin):
#     name = "google_search_console_plugin"
#     hooks = []
#     operators = [GoogleSearchConsoleToS3Operator]
#     executors = []
#     macros = []
#
#
# class S3ToRedshiftPlugin(AirflowPlugin):
#     name = 'S3ToRedshiftPlugin'
#     operators = [S3ToRedshiftOperator]
#     hooks = []
#     executors = []
#     macros = [redshift_auth]
#     admin_views = []
#     flask_blueprints = []
#     menu_links = []
#
#
# class WebpageTesterPlugin(AirflowPlugin):
#     name = "webpage_tester_plugin"
#     hooks = [WptestHook]
#     operators = [WptesterToS3Operator]
#     executors = []
#     macros = []
#
#
# class AMZPlugin(AirflowPlugin):
#     name = "amz_plugin"
#     hooks = []
#     operators = [AmzOrdersToS3Operator, AmzOrderLinesToS3Operator]
#     executors = []
#     macros = []
#
#
# class SHIPRPlugin(AirflowPlugin):
#     name = "shipr_plugin"
#     hooks = []
#     operators = [ShiprocketToS3Operator, ShiprocketShipmentsToS3Operator]
#     executors = []
#     macros = []
