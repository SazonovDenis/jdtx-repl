<%@ page import="jandcode.web.*; jandcode.wax.core.utils.theme.*; jandcode.app.*; jandcode.wax.core.utils.*" %>
<%
    WaxTml th = new WaxTml(this)
    def waxapp = th.app.service(WaxAppService)
%>

<!DOCTYPE html>
<html xmlns:v-bind="http://www.w3.org/1999/xhtml">
<head>
    <meta charset="utf-8">

    <title>Jdtx</title>

    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="icon" type="image/png" sizes="32x32" href="images/favicon-32x32.png">
    <link rel="icon" type="image/png" sizes="16x16" href="images/favicon-16x16.png">
    <link rel="stylesheet" href="css/css.css">
</head>

<body>

<div id="wait_first_load_block" _style="font-family: Arial; font-size: 14px; color: rgb(40, 40, 40);"
     style="display: none;">
    <span style="
    background-repeat: no-repeat;
    background-size: 16px;
    background-image: url(data:image/gif;base64,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);
    padding-left: 20px;">
    </span>
    <span>Подготовка данных...</span>
</div>

<div id="hub_block" style="display: none;">
    <div class="flex-container flex-container-row">
        <div id="state-error-text" class="flex-item state-error-text"
             v-if="state.error_text != null"
             v-on:click="state.error_text = null; state_render_helper = new Date()">
            {{state.error_text}}
        </div>

        <div v-bind:class="state.flash_result == 'error' ? 'flex-item state-error-text' : 'flex-item state-info-text'"
             v-if="state.flash_text != null"
             v-on:click="state.flash_text = null; state_render_helper = new Date()">
            {{state.flash_text}}
        </div>

        <div class="state-render-helper">{{state_render_helper}}</div>
    </div>

</div>

<div id="main_block" style="_display: none;">
    <div class="flex-container flex-container-row">

        <div class="flex-item">
            <span>tableName:</span>
            <input v-model="user_input.tableName">
        </div>

    </div>

    <table>
        <tr>
            <td class="flex-item jdx-field-title" v-for="tableField in tableFields">
                <div v-on:click='onClick_Field(tableField)'
                     v-bind:class="getClass_SelectedField(tableField)">
                    {{tableField}}
                </div>
            </td>
        </tr>
    </table>

    <div class="flex-container flex-container-row">
        <div class="flex-item">
            <h4>Дубликаты:</h4>

            <div class="jdx-grid">
                <div v-bind:class="'jdx-grid-item' + (index==0?'jdx-new-row':'')"
                     v-for="tableField in tableFields">
                    <{{tableField}}>
                </div>

                <template v-for="(itemDouble, itemDoubleIndex) in itemDoubles">
                    <div class="jdx-merge-row">
                        -----
                    </div>
                    <template v-for="recordDouble in itemDouble.records">
                        <div
                                v-bind:class="getClass_SelectedFieldInDouble(recordDouble, tableField) + ' jdx-grid-item ' + (fieldIndex == 0 ? 'jdx-new-row' : '')"
                                v-on:click='onClick_TableField(itemDouble.records, recordDouble, tableField)'
                                v-for="(tableField, fieldIndex) in tableFields">
                            {{recordDouble[tableField]}}
                        </div>

                    </template>

                </template>
            </div>

%{--
            <h5>Дубликаты 1:</h5>

            <div class="jdx-grid">
                <div class="jdx-grid-item jdx-merge-row">0.0-dsdhtytdkjf wltheklth lrterhtkj</div>

                <div class="jdx-grid-item jdx-new-row">1-hdkjf</div>

                <div class="jdx-grid-item">2-ewehdkjf</div>

                <div class="jdx-grid-item jdx-new-row">3-qweqweqw</div>

                <div class="jdx-grid-item">4-hdkjrf</div>

                <div class="jdx-grid-item">5-hewdkjf</div>

                <div class="jdx-grid-item jdx-merge-row">0.1-ahhjtrj rktjkrtj krtjkrt htytdkjf</div>

                <div class="jdx-grid-item jdx-new-row">6-htytdkjf</div>

                <div class="jdx-grid-item">7-rjkwlejrer</div>
            </div>

            <h3>Дубликаты:</h3>

            <div class="flex-container flex-container-col">
                <div v-for="itemDouble in itemDoubles">
                    <table class="xxxxxx">
                        <tr>
                            <td class="flex-item jdx-field-title"
                                v-for="tableField in tableFields">
                                <{{tableField}}>
                            </td>
                        </tr>



                        <tr class="flex-container flex-container-row" v-for="recordDouble in itemDouble.records">
                            <td class="flex-item jdx-field-title"
                                v-bind:class="getClass_SelectedFieldInDouble(recordDouble, tableField)"
                                v-on:click='onClick_TableField(itemDouble.records, recordDouble, tableField)'
                                v-for="tableField in tableFields">
                                {{recordDouble[tableField]}}
                            </td>
                        </tr>
                    </table>
                </div>
            </div>
--}%
        </div>

    </div>

</div>

</body>

<script src="js/vue.js" defer></script>
<script src="js/data.js" defer></script>

</html>