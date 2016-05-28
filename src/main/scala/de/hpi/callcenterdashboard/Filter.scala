package de.hpi.callcenterdashboard

import de.hpi.callcenterdashboard.utility.FormattedDate

case class Filter(
                   startDate: FormattedDate,
                   endDate: FormattedDate,
                   factoryId: String,
                   salesOrganization: String,
                   materialType: String,
                   productHierachyVal: String,
                   countryId: String,
                   regionId: String) {
}
