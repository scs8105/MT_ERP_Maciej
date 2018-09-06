/*
 * [y] hybris Platform
 *
 * Copyright (c) 2000-2014 hybris AG
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of hybris
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with hybris.
 *
 *
 */
package com.hybris.datahub.custom;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.springframework.beans.factory.annotation.Required;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Component;


/**
 * Service to receive IDOCs from SAP systems.
 */
@Produces({ MediaType.APPLICATION_XML, MediaType.TEXT_XML,
		MediaType.APPLICATION_JSON })
@Consumes({ MediaType.APPLICATION_XML, MediaType.TEXT_XML,
		MediaType.APPLICATION_JSON })
@Component
@Path("/idocimport/loop/{count}/counterStatement/{counterStatement}")
public class IDocLoad {
	
	private MessageChannel idocChannel;
	private static final Logger LOGGER = Logger
			.getLogger(IDocLoad.class.getName());

	private long processingDelay;
	
	
	public void setProcessingDelay(long processingDelay) {
		this.processingDelay = processingDelay;
	}

	@POST
	public Response receiveIdoc(@PathParam("count") final String count, @PathParam("counterStatement") final String counterStatement, final String idoc) throws Exception {
		int idocCount = Integer.parseInt(count);
		int idocProcessed=0;
		if (LOGGER.isLoggable(Level.FINE)) {
			LOGGER.fine("[GLOBAL] The processing of " + idocCount + " IDocs will begin now.");
		}
		final long globalStart = System.currentTimeMillis();
		try {
			
			for (int i=1;i<=idocCount;i++) {
				Thread.sleep(processingDelay);
				final String newIdoc=idoc.replace(counterStatement, String.valueOf(i));
				final Map<String, Object> headers = new HashMap<>();
				setHeaders(newIdoc, headers);
				try{
					idocChannel.send(new GenericMessage<>(newIdoc, headers));
				}
				catch(Exception e){
					LOGGER.log(
							Level.SEVERE,
							"Exception occurred during sending IDoc: "
									+ e.getMessage(), e);					
				}
				
				idocProcessed++;
		}
		} catch (final Exception ex) {
			LOGGER.log(
					Level.SEVERE,
					"Exception occurred during processing of IDoc: "
							+ ex.getMessage(), ex);
			throw ex;
		}
		final long globalEnd = System.currentTimeMillis();
		if (LOGGER.isLoggable(Level.FINE)) {
			LOGGER.fine("[GLOBAL] The processing of " + idocProcessed + "/" + idocCount + " took "
					+ (globalEnd - globalStart) + "milliseconds.");
		}
		return Response.ok().build();
	}

	protected void setHeaders(final String idoc,
			final Map<String, Object> headers) {
		final String idocType = getIdocType(idoc);
		headers.put("IDOCTYP", idocType);
	}

	protected String getIdocType(final String idoc) {
		final int start = idoc.indexOf("<IDOCTYP>") + "<IDOCTYP>".length();
		final int end = idoc.indexOf("</IDOCTYP>", start);
		return idoc.substring(start, end);
	}
	
	@Required
	public void setIdocChannel(final MessageChannel idocChannel) {
		this.idocChannel = idocChannel;
	}
	

}
